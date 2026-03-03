import time, re, ccxt, json, os, gc, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ====================== SETTINGS ======================
SETTINGS_FILE = "settings.json"

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE) as f:
                return json.load(f)
        except:
            pass
    return {}

def save_settings(settings):
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=2)
    except:
        pass

saved = load_settings()

# ====================== CONSTANTS ======================
TOP_EXCHANGES = [
    "binance", "okx", "coinbase", "kraken", "bybit", "kucoin",
    "mexc", "bitfinex", "bitget", "gateio", "crypto_com",
    "upbit", "whitebit", "poloniex", "bingx", "lbank",
    "bitstamp", "gemini", "bitrue", "xt", "huobi", "bitmart", "coinex"
]

EXCHANGE_NAMES = {
    "binance": "Binance", "okx": "OKX", "coinbase": "Coinbase",
    "kraken": "Kraken", "bybit": "Bybit", "kucoin": "KuCoin",
    "mexc": "MEXC", "bitfinex": "Bitfinex", "bitget": "Bitget",
    "gateio": "Gate.io", "crypto_com": "Crypto.com", "upbit": "Upbit",
    "whitebit": "WhiteBIT", "poloniex": "Poloniex", "bingx": "BingX",
    "lbank": "LBank", "bitstamp": "Bitstamp", "gemini": "Gemini",
    "bitrue": "Bitrue", "xt": "XT", "huobi": "Huobi", "bitmart": "BitMart", "coinex": "CoinEx"
}

# Binance added — was previously missing, causing fetch_tickers() to silently
# fail with ArgumentsRequired in CCXT v4+, returning {} for all Binance pairs
EXTRA_OPTS = {
    "binance":  {"options": {"defaultType": "spot"}},
    "bybit":    {"options": {"defaultType": "spot"}},
    "okx":      {"options": {"defaultType": "spot"}},
    "bingx":    {"options": {"defaultType": "spot"}},
    "mexc":     {"options": {"defaultType": "spot"}},
    "bitrue":   {"options": {"defaultType": "spot"}},
    "xt":       {"options": {"defaultType": "spot"}},
    "huobi":    {"options": {"defaultType": "spot"}},
    "bitmart":  {"options": {"defaultType": "spot"}},
    "coinex":   {"options": {"defaultType": "spot"}},
}

USD_QUOTES = {"USDT", "USD", "USDC", "BUSD"}
LOW_FEE_CHAIN_PRIORITY = ["TRC20", "BSC", "SOL", "MATIC", "ARB", "OP", "TON", "AVAX", "ETH"]

LEV_REGEX = re.compile(r"\b(\d+[LS]|UP|DOWN|BULL|BEAR)\b", re.IGNORECASE)

# Every alias maps to one canonical name so intersection works correctly.
# Old approach swapped names (BSC->BEP20, BEP20->BSC) so they never matched.
# New: normalize_chain("BSC") == normalize_chain("BEP20") == "BSC"
CHAIN_CANONICAL = {
    "BEP20":    "BSC",
    "BSC":      "BSC",
    "MATIC":    "POLYGON",
    "POLYGON":  "POLYGON",
    "OP":       "OPTIMISM",
    "OPTIMISM": "OPTIMISM",
    "ARB":      "ARBITRUM",
    "ARBITRUM": "ARBITRUM",
    "TRC20":    "TRON",
    "TRON":     "TRON",
}

def normalize_chain(name: str) -> str:
    n = name.upper().strip()
    return CHAIN_CANONICAL.get(n, n)

# ====================== THREAD-SAFE RUNTIME STATE ======================
# All shared globals protected by a lock — concurrent Flask requests in
# production (gunicorn threaded) would otherwise corrupt these
_cache_lock      = threading.Lock()
op_cache         = {}
lifetime_history = {}
last_seen_keys   = set()

# Load semaphore — allows only 1 exchange to run load_markets() at a time.
#
# Without this, when combo threads start simultaneously they each call
# load_markets() at the same time. Each load_markets() fetches and stores
# a full unstripped market catalogue in RAM (7-11MB per exchange) before
# strip_markets() gets a chance to run. With 3 simultaneous loads that is
# a 30MB+ spike all at once, which pushes Render's 512MB free plan over
# the limit before any stripping can help.
#
# With the semaphore:
#   Thread 1 acquires -> loads Binance -> strips (7MB -> 0.5MB) -> releases
#   Thread 2 acquires -> loads Bybit   -> strips (3MB -> 0.2MB) -> releases
#   Thread 3 acquires -> loads MEXC    -> strips (8MB -> 0.6MB) -> releases
#
# Peak memory during loading = 1 unstripped exchange at a time instead of 3.
# Threads still run concurrently for tickers and combo processing.
_load_semaphore  = threading.Semaphore(1)

# ====================== HELPERS ======================
def parse_symbol(symbol: str):
    try:
        base  = symbol.split("/")[0]
        quote = symbol.split("/")[1].split(":")[0]
        return base, quote
    except:
        return None, None

def market_price_from_ticker(t):
    if not t: return None
    last = t.get("last")
    if last is not None:
        try:
            v = float(last)
            if v > 0: return v
        except:
            pass
    bid, ask = t.get("bid"), t.get("ask")
    if bid is not None and ask is not None:
        try: return (float(bid) + float(ask)) / 2.0
        except: return None
    return None

# Reduced from 300s (5 minutes) to 30s — 5-minute-old prices are dangerous
# for arbitrage as the spread could have fully reversed by then
def is_ticker_fresh(t, max_age_sec=30):
    ts = t.get("timestamp")
    if ts is None: return True
    return (int(time.time() * 1000) - int(ts)) <= max_age_sec * 1000

def fmt_usd(x):
    try:
        x = float(x or 0)
        if x < 0: return "$0"
        if x >= 1e9: return f"${x/1e9:.2f}B"
        if x >= 1e6: return f"${x/1e6:.2f}M"
        if x >= 1e3: return f"${x/1e3:.0f}K"
        return f"${x:,.0f}"
    except: return "$0"

def secs_to_label(secs):
    return f"{int(secs)}s" if secs < 90 else f"{secs/60:.1f}m"

def update_lifetime_for_disappeared(current_keys):
    with _cache_lock:
        gone = last_seen_keys - set(current_keys)
        for key in gone:
            trail = op_cache.get(key, [])
            if trail:
                duration = trail[-1][0] - trail[0][0]
                if duration > 0:
                    hist = lifetime_history.setdefault(key, [])
                    hist.append(duration)
                    # Cap at 50 entries — prevents unbounded memory growth
                    # on a long-running server scanning many pairs
                    if len(hist) > 50:
                        lifetime_history[key] = hist[-50:]
        last_seen_keys.clear()
        last_seen_keys.update(current_keys)

def stability_and_expiry(key, current_profit):
    now = time.time()
    with _cache_lock:
        trail = op_cache.get(key, [])
        if not trail:
            op_cache[key] = [(now, current_profit)]
            return "new", "~unknown"
        trail.append((now, current_profit))
        op_cache[key] = trail[-30:]
        duration = trail[-1][0] - trail[0][0]
        observed = f"{secs_to_label(duration)} observed"
        hist = lifetime_history.get(key, [])
        if not hist:
            expiry = "~unknown"
        else:
            avg       = sum(hist) / len(hist)
            remaining = avg - duration
            expiry    = "past avg" if remaining <= 0 else f"~{secs_to_label(remaining)} left"
        return observed, expiry

INFO_VOLUME_CANDIDATES = [
    "quoteVolume", "baseVolume", "vol", "vol24h", "volCcy24h", "volValue",
    "turnover", "turnover24h", "quoteVolume24h", "amount", "value",
    "acc_trade_price_24h", "quote_volume_24h", "base_volume_24h",
]

def safe_usd_volume(ex_id, symbol, ticker, price, all_tickers):
    try:
        base, quote = parse_symbol(symbol)
        if not base or not quote: return 0.0
        q_upper = quote.upper()
        qvol    = ticker.get("quoteVolume")
        bvol    = ticker.get("baseVolume")
        if q_upper in USD_QUOTES and qvol:
            return float(qvol)
        if bvol and price:
            return float(bvol) * float(price)

        info = ticker.get("info") or {}
        raw  = None
        for k in INFO_VOLUME_CANDIDATES:
            val = info.get(k)
            if val is not None:
                try:
                    fval = float(val)
                    if fval > 0:
                        raw = fval
                        break
                except: continue

        if raw is not None:
            if q_upper in USD_QUOTES:
                return float(raw)
            for conv in ["USDT", "USDC", "USD"]:
                conv_t  = all_tickers.get(f"{q_upper}/{conv}")
                conv_px = market_price_from_ticker(conv_t)
                if conv_px:
                    return float(raw) * float(conv_px)
        if qvol:
            for conv in ["USDT", "USDC", "USD"]:
                conv_t  = all_tickers.get(f"{q_upper}/{conv}")
                conv_px = market_price_from_ticker(conv_t)
                if conv_px:
                    return float(qvol) * float(conv_px)
        return 0.0
    except:
        return 0.0

# Now accepts slim_markets dict {symbol: {spot, active, taker}} instead of
# full ccxt exchange object — ex.markets is wiped after stripping to free memory
def symbol_ok(symbol, slim_markets):
    try:
        m = slim_markets.get(symbol, {})
        # Default False — unknown market type should not pass through
        if not m or not m.get("spot", False): return False
        base, quote = parse_symbol(symbol)
        if not base or not quote: return False
        if quote.upper() not in USD_QUOTES: return False
        if LEV_REGEX.search(symbol): return False
        if m.get("active") is False: return False
        return True
    except:
        return False

# Now accepts slim_currencies dicts {coin: {networks}} instead of full ccxt
# exchange objects — ex.currencies is wiped after stripping to free memory
def choose_common_chain(b_slim_curr, s_slim_curr, coin, exclude_chains, include_all_chains):
    try:
        nets1_raw = (b_slim_curr.get(coin) or {}).get("networks") or {}
        nets2_raw = (s_slim_curr.get(coin) or {}).get("networks") or {}

        # When either exchange has no network data (requires API keys to populate),
        # return Unverified instead of No chain so the opportunity is still
        # surfaced with a warning rather than being silently dropped entirely
        if not nets1_raw or not nets2_raw:
            return "Unverified", "unverified", "unverified"

        # normalize_chain maps to canonical names so aliases match:
        # BSC and BEP20 both map to "BSC", MATIC and Polygon both map to "POLYGON"
        nets1_norm = {normalize_chain(k): (k, v) for k, v in nets1_raw.items()}
        nets2_norm = {normalize_chain(k): (k, v) for k, v in nets2_raw.items()}

        common_norm = set(nets1_norm.keys()) & set(nets2_norm.keys())
        if not common_norm:
            return "No chain", "no", "no"

        exclude_norm   = {normalize_chain(c) for c in exclude_chains} if not include_all_chains else set()
        preferred_norm = [normalize_chain(n) for n in LOW_FEE_CHAIN_PRIORITY if normalize_chain(n) not in exclude_norm]

        best_norm = next((p for p in preferred_norm if p in common_norm), None)
        if not best_norm:
            candidates = [c for c in common_norm if c not in exclude_norm]
            if not candidates: return "No chain", "no", "no"
            best_norm = sorted(candidates)[0]

        _, info1 = nets1_norm[best_norm]
        _, info2 = nets2_norm[best_norm]
        w_ok = "yes" if info1.get("withdraw") else "no"
        d_ok = "yes" if info2.get("deposit")  else "no"
        return best_norm, w_ok, d_ok
    except:
        return "Unknown", "no", "no"

def fetch_tickers_safe(ex, name, logger=None):
    for attempt in range(3):
        try:
            return ex.fetch_tickers()
        except Exception as e:
            if attempt == 2:
                if logger:
                    logger(f"fetch_tickers failed for {name}: {str(e)[:80]}")
                return {}
            time.sleep((2 ** attempt) * 1.5)
    return {}


# ====================== MEMORY STRIPPING ======================
#
# After load_markets(), ccxt stores a full object per market containing:
# precision, limits, filters, maker fee, permissions, info (raw exchange
# response duplicated in full) and many other fields the scanner never reads.
#
# Memory cost before stripping:
#   ~1000 bytes per market x 2200 markets x 3-5x Python overhead = ~7-11MB per exchange
#
# Memory cost after stripping to {spot, active, taker} only:
#   ~60 bytes per market x 2200 markets x 3-5x Python overhead = ~0.4-0.7MB per exchange
#
# Same applies to tickers — ccxt stores high, low, open, close, vwap, change,
# percentage, datetime, bidVolume, askVolume etc. that the scanner never uses.
#
# Memory cost before stripping:
#   ~900 bytes per ticker x 2200 tickers x 3-5x Python overhead = ~6-10MB per exchange
#
# Memory cost after stripping to {last, bid, ask, timestamp, quoteVolume, baseVolume, info}:
#   ~200 bytes per ticker x 2200 tickers x 3-5x Python overhead = ~1.3-2.2MB per exchange
#
# Combined saving across 4 exchanges: roughly 50-70MB freed immediately after load

def strip_markets(ex):
    """
    Extract only the 3 fields symbol_ok() and process_combo() need,
    then wipe ex.markets and ex.currencies immediately to free memory.

    Returns:
        slim_markets    -- {symbol: {spot, active, taker}}
        slim_currencies -- {coin: {networks: {...}}}
    """
    slim_markets = {
        sym: {
            "spot":   m.get("spot",   False),
            "active": m.get("active", True),
            "taker":  m.get("taker",  0.001),
        }
        for sym, m in ex.markets.items()
    }

    # Only keep the networks sub-dict per coin — fees, limits, precision,
    # info, name, id inside currencies are never used by the scanner
    slim_currencies = {
        coin: {"networks": (c.get("networks") or {}) if c else {}}
        for coin, c in (ex.currencies or {}).items()
    }

    # Wipe full data from the exchange object immediately
    ex.markets.clear()
    try:
        ex.currencies.clear()
    except Exception:
        pass

    return slim_markets, slim_currencies


def strip_tickers(raw_tickers):
    """
    Extract only the fields the scanner uses from fetch_tickers() response,
    then delete the raw dict immediately to free memory.

    Fields kept:
        last, bid, ask   -- price calculation
        timestamp        -- freshness check (is_ticker_fresh)
        quoteVolume      -- USD volume when quote is a stablecoin
        baseVolume       -- USD volume fallback (multiplied by price)
        info             -- kept for exchange-specific volume field fallbacks
                           (volValue, turnover24h, volCcy24h etc. vary by exchange)

    Everything else discarded: high, low, open, close, vwap, change,
    percentage, average, previousClose, datetime, bidVolume, askVolume
    """
    slim = {
        sym: {
            "last":        t.get("last"),
            "bid":         t.get("bid"),
            "ask":         t.get("ask"),
            "timestamp":   t.get("timestamp"),
            "quoteVolume": t.get("quoteVolume"),
            "baseVolume":  t.get("baseVolume"),
            "info":        t.get("info") or {},
        }
        for sym, t in raw_tickers.items()
    }
    del raw_tickers
    return slim


# ====================== EXCHANGE CACHE (within one scan) ======================
# Shared across all combo worker threads so each exchange is only loaded
# and fetched once per scan even if it appears in multiple combos.
# All methods are thread-safe via an internal lock.

class ExchangeCache:
    def __init__(self):
        self._lock         = threading.Lock()
        self._ex_objs      = {}   # ex_id -> ccxt exchange object (kept only for fetch_tickers)
        self._slim_markets = {}   # ex_id -> {symbol: {spot, active, taker}}
        self._slim_currs   = {}   # ex_id -> {coin: {networks}}
        self._slim_tickers = {}   # ex_id -> {symbol: {last, bid, ask, ...}}
        self._failed       = set()

    def get_slim_data(self, ex_id, logger):
        """
        Load exchange, strip markets and currencies immediately, cache and return.
        Thread-safe — if two threads request the same exchange simultaneously,
        only one will do the HTTP load; the other waits and gets the cached result.
        Returns (slim_markets, slim_currencies) or (None, None) on failure.
        """
        with self._lock:
            if ex_id in self._failed:
                return None, None
            if ex_id in self._slim_markets:
                return self._slim_markets[ex_id], self._slim_currs[ex_id]

        # Load outside lock so other combo threads are not blocked during
        # the HTTP calls that load_markets() makes.
        # The _load_semaphore ensures only 1 exchange loads at a time —
        # prevents simultaneous load_markets() calls from spiking RAM with
        # multiple full unstripped market catalogues before stripping runs.
        try:
            opts = {"enableRateLimit": True, "timeout": 30000}
            opts.update(EXTRA_OPTS.get(ex_id, {}))
            ex = getattr(ccxt, ex_id)(opts)

            with _load_semaphore:
                # Only 1 thread can be inside here at a time.
                # load_markets() runs, strip_markets() immediately frees the bulk,
                # then the semaphore releases so the next exchange can load.
                ex.load_markets()
                name      = EXCHANGE_NAMES.get(ex_id, ex_id)
                mkt_count = len(ex.markets)
                slim_m, slim_c = strip_markets(ex)
                gc.collect()

            with self._lock:
                self._ex_objs[ex_id]      = ex
                self._slim_markets[ex_id] = slim_m
                self._slim_currs[ex_id]   = slim_c

            logger(f"Loaded {name} ({mkt_count} markets, stripped to slim)")
            return slim_m, slim_c

        except Exception as e:
            logger(f"Skipped {EXCHANGE_NAMES.get(ex_id, ex_id)}: {str(e)[:80]}")
            with self._lock:
                self._failed.add(ex_id)
            return None, None

    def get_slim_tickers(self, ex_id, logger):
        """
        Fetch tickers, strip to only needed fields immediately, cache and return.
        Thread-safe — same exchange only fetched once across all combo threads.
        Returns slim ticker dict or {} on failure.
        """
        with self._lock:
            if ex_id in self._failed:
                return {}
            if ex_id in self._slim_tickers:
                return self._slim_tickers[ex_id]
            ex = self._ex_objs.get(ex_id)

        if ex is None:
            return {}

        name = EXCHANGE_NAMES.get(ex_id, ex_id)

        # Fetch outside lock
        raw = fetch_tickers_safe(ex, name, logger)
        if not raw:
            return {}

        raw_count = len(raw)

        # Strip tickers immediately after fetching.
        # Frees ~6-10MB per exchange worth of unused fields and deletes
        # the full raw dict right after extraction.
        slim = strip_tickers(raw)
        gc.collect()

        with self._lock:
            self._slim_tickers[ex_id] = slim

        logger(f"Fetched tickers -> {name} ({raw_count} tickers, stripped to slim)")
        return slim

    def cleanup(self):
        """Wipe all cached data after the scan to help GC reclaim memory."""
        with self._lock:
            for ex in self._ex_objs.values():
                try:
                    if hasattr(ex, 'markets'):    ex.markets.clear()
                    if hasattr(ex, 'currencies'): ex.currencies.clear()
                    if hasattr(ex, 'tickers'):    ex.tickers.clear()
                except Exception:
                    pass
            self._ex_objs.clear()
            self._slim_markets.clear()
            self._slim_currs.clear()
            self._slim_tickers.clear()
            self._failed.clear()


# ====================== SINGLE COMBO PROCESSOR ======================
def process_combo(b_id, s_id, cache, settings, logger):
    """
    Process one (buy_exchange, sell_exchange) pair.
    Runs inside a thread pool — all data comes from the shared ExchangeCache.
    Returns (list_of_opportunity_dicts, list_of_keys) for this combo.
    """
    min_p       = settings.get("min_profit", 1.0)
    max_p       = settings.get("max_profit", 20.0)
    min_vol     = settings.get("min_24h_vol_usd", 100000.0)
    exclude     = settings.get("exclude_chains", ["ETH"])
    include_all = settings.get("include_all_chains", False)
    MAX_PAIRS   = 200

    b_slim_m, b_slim_c = cache.get_slim_data(b_id, logger)
    s_slim_m, s_slim_c = cache.get_slim_data(s_id, logger)
    if b_slim_m is None or s_slim_m is None:
        return [], []

    b_tk = cache.get_slim_tickers(b_id, logger)
    s_tk = cache.get_slim_tickers(s_id, logger)
    if not b_tk or not s_tk:
        return [], []

    # Intersect using slim_markets keys (ex.markets was wiped after stripping)
    common  = set(b_slim_m.keys()) & set(s_slim_m.keys())
    symbols = [s for s in common if symbol_ok(s, b_slim_m) and symbol_ok(s, s_slim_m)]

    if not symbols:
        return [], []

    def vol_score(sym):
        bt  = b_tk.get(sym)
        st_ = s_tk.get(sym)
        pb  = market_price_from_ticker(bt) or 0
        ps  = market_price_from_ticker(st_) or 0
        return (
            safe_usd_volume(b_id, sym, bt, pb, b_tk) +
            safe_usd_volume(s_id, sym, st_, ps, s_tk)
        )

    symbols.sort(key=vol_score, reverse=True)
    symbols = symbols[:MAX_PAIRS]

    combo_results = []
    current_keys  = []

    for sym in symbols:
        bt  = b_tk.get(sym)
        st_ = s_tk.get(sym)
        if not bt or not st_: continue
        if not is_ticker_fresh(bt) or not is_ticker_fresh(st_): continue

        bp = market_price_from_ticker(bt)
        sp = market_price_from_ticker(st_)
        if not bp or not sp: continue

        if abs(sp - bp) / bp > 0.5: continue

        # Taker fee from slim_markets — the only fee field kept after stripping
        b_fee = b_slim_m.get(sym, {}).get("taker", 0.001)
        s_fee = s_slim_m.get(sym, {}).get("taker", 0.001)

        spread = (sp - bp) / bp * 100
        profit = spread - (b_fee * 100 + s_fee * 100)
        if profit < min_p or profit > max_p: continue

        b_vol = safe_usd_volume(b_id, sym, bt, bp, b_tk)
        s_vol = safe_usd_volume(s_id, sym, st_, sp, s_tk)
        if b_vol < min_vol or s_vol < min_vol: continue

        base, quote = parse_symbol(sym)

        # Pass slim_currencies (ex.currencies was wiped after stripping)
        chain, w_ok, d_ok = choose_common_chain(
            b_slim_c, s_slim_c, base, exclude, include_all
        )

        # Only block confirmed failures — "unverified" (no API key, network
        # data unavailable) is allowed through so exchanges like Binance/Bybit/
        # MEXC/CoinEx still surface opportunities with a warning
        if w_ok == "no" or d_ok == "no": continue
        if not include_all and chain in ("No chain", "Unknown"): continue

        key = f"{sym}|{b_id}>{s_id}"
        current_keys.append(key)
        obs, exp = stability_and_expiry(key, profit)

        combo_results.append({
            "Pair":                 sym,
            "Quote":                quote,
            "Buy@":                 EXCHANGE_NAMES.get(b_id, b_id),
            "Buy Price":            round(bp, 10),
            "Sell@":                EXCHANGE_NAMES.get(s_id, s_id),
            "Sell Price":           round(sp, 10),
            "Spread %":             round(spread, 4),
            "Profit % After Fees":  round(profit, 4),
            "Buy Vol (24h)":        fmt_usd(b_vol),
            "Sell Vol (24h)":       fmt_usd(s_vol),
            "Withdraw?":            w_ok,
            "Deposit?":             d_ok,
            "Blockchain":           chain,
            "Stability":            obs,
            "Est. Expiry":          exp,
        })

    logger(
        f"{EXCHANGE_NAMES.get(b_id, b_id)} -> {EXCHANGE_NAMES.get(s_id, s_id)}: "
        f"{len(combo_results)} opportunities"
    )
    return combo_results, current_keys


# ====================== CORE SCAN ======================
def run_scan(settings, logger):
    buy_ids  = settings.get("buy_exchanges", [])
    sell_ids = settings.get("sell_exchanges", [])

    logger("Starting scan (3 combos at a time, slim market+ticker mode)")
    logger(f"Buy: {buy_ids} | Sell: {sell_ids}")

    if not buy_ids or not sell_ids:
        logger("Need at least one buy and sell exchange")
        return []

    combos = [(b, s) for b in buy_ids for s in sell_ids if b != s]
    if not combos:
        logger("No valid combos (all buy/sell exchanges are the same)")
        return []

    logger(f"{len(combos)} combos to process")

    # One shared cache for the whole scan.
    # Exchange data is loaded once, stripped immediately, then reused
    # across all combo threads — Binance/KuCoin/etc. never fetched twice.
    cache        = ExchangeCache()
    all_results  = []
    all_keys     = []
    results_lock = threading.Lock()

    # max_workers=2 matches gunicorn's --threads 2 setting.
    # Using 3 would create 5 total threads (3 executor + 2 gunicorn)
    # competing for the same 512MB, amplifying memory pressure.
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(process_combo, b, s, cache, settings, logger): (b, s)
            for b, s in combos
        }
        for future in as_completed(futures):
            b_id, s_id = futures[future]
            try:
                combo_results, combo_keys = future.result()
                with results_lock:
                    all_results.extend(combo_results)
                    all_keys.extend(combo_keys)
            except Exception as e:
                logger(f"Combo {b_id}->{s_id} error: {str(e)[:80]}")

    # Full set of keys from all combos that actually ran — fixes the bug where
    # skipped combos caused their keys to always appear as disappeared and
    # corrupted lifetime and expiry estimates
    update_lifetime_for_disappeared(all_keys)

    # Wipe all exchange data and force GC to reclaim memory
    cache.cleanup()
    gc.collect()

    all_results.sort(key=lambda x: x["Profit % After Fees"], reverse=True)
    logger(f"Scan complete -- {len(all_results)} total opportunities")
    return all_results


# ====================== ROUTES ======================
@app.route('/')
def index():
    base_dir  = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, 'frontend.html')
    with open(html_path, 'r') as f:
        html = f.read()
    return render_template_string(html, TOP_EXCHANGES=TOP_EXCHANGES, EXCHANGE_NAMES=EXCHANGE_NAMES)

@app.route('/api/scan', methods=['POST'])
def api_scan():
    settings = request.get_json() or {}
    logs     = []

    def logger(msg):
        ts   = time.strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line)
        logs.append(line)

    results  = run_scan(settings, logger)

    # Only save settings when scan had valid inputs — prevents bad or empty
    # settings from overwriting good saved settings on a failed scan
    buy_ids  = settings.get("buy_exchanges", [])
    sell_ids = settings.get("sell_exchanges", [])
    if buy_ids and sell_ids and results is not None:
        save_settings(settings)

    return jsonify({"results": results, "logs": logs})

@app.route('/api/exchanges')
def get_exchanges():
    return jsonify({
        "exchanges": TOP_EXCHANGES,
        "names":     EXCHANGE_NAMES
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
