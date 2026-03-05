import time, re, ccxt, json, os, gc, threading, urllib.request, urllib.error
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
    "bitrue": "Bitrue", "xt": "XT", "huobi": "Huobi",
    "bitmart": "BitMart", "coinex": "CoinEx"
}

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
LOW_FEE_CHAIN_PRIORITY = [
    "TRC20", "BSC", "SOL", "MATIC", "ARB", "OP", "TON", "AVAX", "ETH"
]

LEV_REGEX = re.compile(
    r"(^|[^A-Z0-9])(UP|DOWN|BULL|BEAR)([^A-Z0-9]|$)"
    r"|\d+[LSX]"
    r"|(UP|DOWN|BULL|BEAR)$"
    r"|^(UP|DOWN|BULL|BEAR)",
    re.IGNORECASE
)

CHAIN_CANONICAL = {
    "BEP20":    "BSC",      "BSC":      "BSC",
    "MATIC":    "POLYGON",  "POLYGON":  "POLYGON",
    "OP":       "OPTIMISM", "OPTIMISM": "OPTIMISM",
    "ARB":      "ARBITRUM", "ARBITRUM": "ARBITRUM",
    "TRC20":    "TRON",     "TRON":     "TRON",
}

def normalize_chain(name: str) -> str:
    return CHAIN_CANONICAL.get(name.upper().strip(), name.upper().strip())

# ====================== COINGECKO NETWORK FALLBACK ======================
#
# Strategy: ALL CoinGecko data is pre-fetched in a dedicated phase BEFORE
# Phase 2 begins. This avoids per-symbol HTTP calls inside the tight scan
# loop which would immediately hit CoinGecko's 30 req/min rate limit and
# cause every opportunity to fall back to "Unverified".
#
# Flow:
#   1. After Phase 1 (all exchanges loaded), collect every unique base coin
#      from the common symbol sets across all planned combos.
#   2. Call prefetch_cg_networks_bulk() which:
#        a. Fetches /coins/list once to build symbol → CG id map.
#        b. Fetches /coins/{id} for each needed coin with a 2.2s delay
#           between requests (~27/min — safely under the 30/min free limit).
#        c. Stores results in _cg_coin_cache.
#   3. Phase 2 calls choose_common_chain() → get_cg_networks() which
#      returns the pre-cached result instantly (no HTTP).
#
# Limits of this approach:
#   - Cannot confirm per-exchange real-time withdrawal/deposit status without
#     API keys. We treat "coin exists on this chain" as "transfer available".
#   - /coins/list returns ~13 000 entries; one-time cost at scan start.

CG_PLATFORM_TO_CHAIN = {
    "ethereum":              "ETH",
    "tron":                  "TRON",
    "binance-smart-chain":   "BSC",
    "solana":                "SOL",
    "polygon-pos":           "POLYGON",
    "arbitrum-one":          "ARBITRUM",
    "optimistic-ethereum":   "OPTIMISM",
    "the-open-network":      "TON",
    "avalanche":             "AVAX",
    "bitcoin":               "BTC",
    "base":                  "BASE",
    "sui":                   "SUI",
    "aptos":                 "APTOS",
}

_cg_lock             = threading.Lock()
_cg_coins_list       = {}     # symbol.upper() -> coingecko_id  (populated once)
_cg_list_loaded      = False  # FIX: sentinel — True once /coins/list was attempted
                               # (even if it failed), so we never retry it in a
                               # tight loop after a transient CG error.
_cg_coin_cache       = {}     # coingecko_id -> {chain: {withdraw:T,deposit:T}, ...}
_cg_cache_ts         = {}     # coingecko_id -> epoch_sec of last fetch
_CG_CACHE_TTL        = 86400  # 24 h — chain support rarely changes
_CG_BASE             = "https://api.coingecko.com/api/v3"
_CG_HEADERS          = {"User-Agent": "ArbitrageScanner/1.0", "Accept": "application/json"}

# How long to pause between per-coin CG requests during pre-fetch.
# Free tier: 30 req/min → 2.0 s/req. We use 2.2 s for a small safety margin.
_CG_RATE_DELAY       = 2.2

def _cg_get(path: str):
    """Simple HTTP GET to CoinGecko. Returns parsed JSON or None on any error."""
    try:
        req = urllib.request.Request(_CG_BASE + path, headers=_CG_HEADERS)
        with urllib.request.urlopen(req, timeout=12) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None

def _ensure_cg_coins_list(logger=None):
    """
    Fetch /coins/list once and build symbol → CG id map.

    FIX vs original:
    - HTTP is done OUTSIDE the lock to avoid holding the mutex for 5-10 s.
    - _cg_list_loaded sentinel prevents re-trying after a failed fetch, which
      would flood CoinGecko with retries on every get_cg_networks() call.
    """
    global _cg_coins_list, _cg_list_loaded

    with _cg_lock:
        if _cg_list_loaded:
            return        # already attempted (success or fail) — do nothing

    # Fetch outside the lock so other threads are not blocked during HTTP.
    if logger:
        logger("CoinGecko: fetching /coins/list …")
    data = _cg_get("/coins/list")

    with _cg_lock:
        _cg_list_loaded = True   # mark attempted regardless of outcome
        if not data:
            if logger:
                logger("CoinGecko: /coins/list failed — chain validation will rely only on exchange data")
            return
        mapping = {}
        for coin in data:
            sym = coin.get("symbol", "").upper()
            cid = coin.get("id", "")
            # For duplicate symbols keep the entry with the shorter ID
            # (e.g. "bitcoin" wins over "bitcoin-sv" for BTC).
            if sym not in mapping or len(cid) < len(mapping[sym]):
                mapping[sym] = cid
        _cg_coins_list = mapping
        if logger:
            logger(f"CoinGecko: /coins/list OK — {len(mapping)} symbols indexed")

def get_cg_networks(coin: str) -> dict:
    """
    Return cached network dict for `coin`, e.g.
    {"ETH": {"withdraw": True, "deposit": True}, "BSC": {...}, ...}
    Returns {} if CoinGecko has no data or /coins/list was not loaded.

    During a scan this should ONLY be called after prefetch_cg_networks_bulk()
    has already populated the cache for the relevant coins. That way this
    function never makes an HTTP call mid-scan and cannot be rate-limited.
    """
    coin_upper = coin.upper()

    with _cg_lock:
        if not _cg_list_loaded:
            return {}   # list was never fetched — caller should prefetch first
        cg_id = _cg_coins_list.get(coin_upper)
        if not cg_id:
            return {}
        now = time.time()
        if cg_id in _cg_coin_cache and (now - _cg_cache_ts.get(cg_id, 0)) < _CG_CACHE_TTL:
            return _cg_coin_cache[cg_id]

    # Cache miss — fetch (only happens if the coin was not in the prefetch list
    # or the cache expired).  Single fetch, no lock held during HTTP.
    data = _cg_get(f"/coins/{cg_id}?localization=false&tickers=false"
                   f"&market_data=false&community_data=false&developer_data=false")
    if not data:
        return {}

    platforms = data.get("platforms") or {}
    networks  = {}
    for platform, address in platforms.items():
        if not address:
            continue
        chain = CG_PLATFORM_TO_CHAIN.get(platform.lower())
        if not chain:
            chain = normalize_chain(platform.replace("-", "_").upper())
        networks[chain] = {"withdraw": True, "deposit": True}

    with _cg_lock:
        _cg_coin_cache[cg_id] = networks
        _cg_cache_ts[cg_id]   = time.time()

    return networks


def prefetch_cg_networks_bulk(coins_iterable, logger=None):
    """
    Pre-populate the CoinGecko cache for every coin in `coins_iterable`
    before Phase 2 starts.

    WHY THIS IS NEEDED:
    Without pre-fetching, get_cg_networks() is called once per symbol inside
    the Phase 2 combo loop. With hundreds of symbols, this fires dozens of
    HTTP requests in rapid succession, immediately exceeding CoinGecko's
    30 req/min free tier. After the first ~30 calls CG starts returning 429s,
    _cg_get() catches the exception and returns None, and every subsequent
    coin falls back to "Unverified" — exactly the behaviour seen in the
    screenshot.

    By pre-fetching with a _CG_RATE_DELAY sleep between requests, all data
    is in memory before Phase 2 begins. Phase 2 then only does dict lookups.

    The cost is linear time: ~2.2 s per uncached coin. For 50 unique coins
    that is ~110 s. We cap at MAX_CG_PREFETCH to avoid runaway delays.
    """
    MAX_CG_PREFETCH = 80   # caps additional wait at ~80 * 2.2 s ≈ 176 s

    # Ensure /coins/list is loaded first (one-time cost).
    _ensure_cg_coins_list(logger)

    with _cg_lock:
        if not _cg_coins_list:
            if logger:
                logger("CoinGecko: no coin index available — skipping prefetch")
            return

    coins = list(dict.fromkeys(c.upper() for c in coins_iterable))  # dedupe, preserve order

    now       = time.time()
    to_fetch  = []
    skipped   = 0

    with _cg_lock:
        for coin in coins:
            cg_id = _cg_coins_list.get(coin)
            if not cg_id:
                skipped += 1
                continue
            if cg_id in _cg_coin_cache and (now - _cg_cache_ts.get(cg_id, 0)) < _CG_CACHE_TTL:
                continue   # already fresh in cache
            to_fetch.append((coin, cg_id))

    if not to_fetch:
        if logger:
            logger(f"CoinGecko: all {len(coins) - skipped} coins already cached "
                   f"({skipped} not in CG index)")
        return

    total = min(len(to_fetch), MAX_CG_PREFETCH)
    to_fetch = to_fetch[:total]

    eta = total * _CG_RATE_DELAY
    if logger:
        logger(f"CoinGecko: pre-fetching {total} coins "
               f"(~{eta:.0f}s, rate-limited to ~27 req/min) …")

    ok = 0
    for i, (coin, cg_id) in enumerate(to_fetch):
        data = _cg_get(
            f"/coins/{cg_id}?localization=false&tickers=false"
            f"&market_data=false&community_data=false&developer_data=false"
        )
        if data:
            platforms = data.get("platforms") or {}
            networks  = {}
            for platform, address in platforms.items():
                if not address:
                    continue
                chain = CG_PLATFORM_TO_CHAIN.get(platform.lower())
                if not chain:
                    chain = normalize_chain(platform.replace("-", "_").upper())
                networks[chain] = {"withdraw": True, "deposit": True}
            with _cg_lock:
                _cg_coin_cache[cg_id] = networks
                _cg_cache_ts[cg_id]   = time.time()
            if networks:
                ok += 1
        # Always sleep between requests, even on failure, to respect rate limit.
        if i < total - 1:
            time.sleep(_CG_RATE_DELAY)

    if logger:
        logger(f"CoinGecko: prefetch complete — "
               f"{ok}/{total} coins have chain data "
               f"({skipped} not in CG index)")


# ====================== THREAD-SAFE RUNTIME STATE ======================
_cache_lock      = threading.Lock()
_scan_lock       = threading.Lock()
op_cache         = {}
lifetime_history = {}
last_seen_keys   = set()

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

def is_ticker_fresh(t, reference_ms=None, max_age_sec=30):
    ts = t.get("timestamp")
    if ts is None: return True
    ref = reference_ms if reference_ms is not None else int(time.time() * 1000)
    return (ref - int(ts)) <= max_age_sec * 1000

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
            trail = op_cache.pop(key, [])
            if trail:
                duration = trail[-1][0] - trail[0][0]
                if duration > 0:
                    hist = lifetime_history.setdefault(key, [])
                    hist.append(duration)
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
        hist     = lifetime_history.get(key, [])
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
    "q", "v",
]
_INFO_KEYS_NEEDED    = set(INFO_VOLUME_CANDIDATES)
_INFO_BASE_VOL_KEYS  = {"vol", "vol24h", "volCcy24h", "baseVolume", "base_volume_24h", "v", "volume"}
_INFO_QUOTE_VOL_KEYS = {"amount", "quoteVolume24h", "quote_volume_24h",
                        "acc_trade_price_24h", "volValue", "turnover", "turnover24h", "q", "quoteVolume"}

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

        def try_keys(keys):
            for k in keys:
                val = info.get(k)
                if val is not None:
                    try:
                        fval = float(val)
                        if fval > 0:
                            return fval
                    except:
                        continue
            return None

        raw_quote = try_keys(_INFO_QUOTE_VOL_KEYS)
        if raw_quote is not None:
            if q_upper in USD_QUOTES:
                return raw_quote
            for conv in ["USDT", "USDC", "USD"]:
                conv_t  = all_tickers.get(f"{q_upper}/{conv}")
                conv_px = market_price_from_ticker(conv_t)
                if conv_px:
                    return raw_quote * conv_px

        raw_base = try_keys(_INFO_BASE_VOL_KEYS)
        if raw_base is not None and price:
            usd_vol = raw_base * float(price)
            if q_upper in USD_QUOTES:
                return usd_vol
            for conv in ["USDT", "USDC", "USD"]:
                conv_t  = all_tickers.get(f"{q_upper}/{conv}")
                conv_px = market_price_from_ticker(conv_t)
                if conv_px:
                    return raw_base * conv_px

        if qvol:
            for conv in ["USDT", "USDC", "USD"]:
                conv_t  = all_tickers.get(f"{q_upper}/{conv}")
                conv_px = market_price_from_ticker(conv_t)
                if conv_px:
                    return float(qvol) * conv_px
        return 0.0
    except:
        return 0.0

def symbol_ok(symbol, slim_markets):
    try:
        m = slim_markets.get(symbol, {})
        if not m: return False
        is_spot = m.get("spot", False) or m.get("type", "") == "spot"
        if not is_spot: return False
        base, quote = parse_symbol(symbol)
        if not base or not quote: return False
        if quote.upper() not in USD_QUOTES: return False
        if LEV_REGEX.search(base): return False
        if m.get("active") is False: return False
        return True
    except:
        return False

def choose_common_chain(b_slim_curr, s_slim_curr, coin, exclude_chains, include_all):
    try:
        nets1_raw = (b_slim_curr.get(coin) or {}).get("networks") or {}
        nets2_raw = (s_slim_curr.get(coin) or {}).get("networks") or {}

        # Fall back to CoinGecko data for exchanges that don't expose
        # network info without API keys (Binance, Bybit, MEXC, BingX, etc.).
        # By this point prefetch_cg_networks_bulk() should have already
        # populated the cache, so get_cg_networks() is a pure dict lookup.
        if not nets1_raw:
            nets1_raw = get_cg_networks(coin)
        if not nets2_raw:
            nets2_raw = get_cg_networks(coin)

        if not nets1_raw or not nets2_raw:
            return "Unverified", "unverified", "unverified"

        def merge_networks(raw_nets):
            merged = {}
            for raw_k, v in raw_nets.items():
                norm = normalize_chain(raw_k)
                if norm in merged:
                    existing = merged[norm][1]
                    combined = dict(existing)
                    combined["withdraw"] = existing.get("withdraw") or v.get("withdraw")
                    combined["deposit"]  = existing.get("deposit")  or v.get("deposit")
                    merged[norm] = (raw_k, combined)
                else:
                    merged[norm] = (raw_k, v)
            return merged

        nets1_norm = merge_networks(nets1_raw)
        nets2_norm = merge_networks(nets2_raw)

        common_norm = set(nets1_norm.keys()) & set(nets2_norm.keys())
        if not common_norm:
            return "No chain", "no", "no"
        exclude_norm   = {normalize_chain(c) for c in exclude_chains} if not include_all else set()
        preferred_norm = [
            normalize_chain(n) for n in LOW_FEE_CHAIN_PRIORITY
            if normalize_chain(n) not in exclude_norm
        ]
        best_norm = next((p for p in preferred_norm if p in common_norm), None)
        if not best_norm:
            candidates = [c for c in common_norm if c not in exclude_norm]
            if not candidates: return "No chain", "no", "no"
            best_norm = sorted(candidates)[0]
        _, info1 = nets1_norm[best_norm]
        _, info2 = nets2_norm[best_norm]

        w_ok = "no" if info1.get("withdraw") is False else "yes"
        d_ok = "no" if info2.get("deposit")  is False else "yes"

        return best_norm, w_ok, d_ok
    except:
        return "Unknown", "no", "no"

def fetch_tickers_safe(ex, name, logger):
    for attempt in range(3):
        try:
            return ex.fetch_tickers()
        except Exception as e:
            if attempt == 2:
                logger(f"fetch_tickers failed for {name}: {str(e)[:80]}")
                return {}
            time.sleep((2 ** attempt) * 1.5)

# ====================== MEMORY STRIPPING ======================
def strip_markets(ex):
    slim_markets = {
        sym: {
            "spot":   m.get("spot",   False),
            "type":   m.get("type",   ""),
            "active": m.get("active", True),
            "taker":  m.get("taker",  0.001),
        }
        for sym, m in ex.markets.items()
    }
    slim_currencies = {
        coin: {"networks": (c.get("networks") or {}) if c else {}}
        for coin, c in (ex.currencies or {}).items()
    }
    ex.markets.clear()
    try:
        ex.currencies.clear()
    except Exception:
        pass
    return slim_markets, slim_currencies

def strip_tickers(raw_tickers, symbol_map=None):
    slim = {}
    for sym, t in raw_tickers.items():
        unified = sym
        if symbol_map and "/" not in sym:
            unified = symbol_map.get(sym, sym)
        slim[unified] = {
            "last":        t.get("last"),
            "bid":         t.get("bid"),
            "ask":         t.get("ask"),
            "timestamp":   t.get("timestamp"),
            "quoteVolume": t.get("quoteVolume"),
            "baseVolume":  t.get("baseVolume"),
            "info":        {k: v for k, v in (t.get("info") or {}).items()
                            if k in _INFO_KEYS_NEEDED},
        }
    del raw_tickers
    return slim

# ====================== CORE SCAN - SEQUENTIAL TWO-PHASE ======================
def run_scan(settings, logger):
    buy_ids     = settings.get("buy_exchanges", [])
    sell_ids    = settings.get("sell_exchanges", [])
    min_p       = settings.get("min_profit", 1.0)
    max_p       = settings.get("max_profit", 20.0)
    min_vol     = settings.get("min_24h_vol_usd", 100000.0)
    exclude     = settings.get("exclude_chains", ["ETH"])
    include_all = settings.get("include_all_chains", False)
    MAX_PAIRS   = 200

    logger("Starting scan - Sequential Two-Phase")
    logger(f"Buy: {buy_ids}")
    logger(f"Sell: {sell_ids}")

    if not buy_ids or not sell_ids:
        logger("Need at least one buy and one sell exchange")
        return []

    all_ex_ids = list(dict.fromkeys(buy_ids + sell_ids))
    combos     = [(b, s) for b in buy_ids for s in sell_ids if b != s]

    if not combos:
        logger("No valid combos")
        return []

    logger(f"{len(all_ex_ids)} unique exchanges, {len(combos)} combos")

    # ── PHASE 1 ───────────────────────────────────────────────────────────────
    slim_markets    = {}
    slim_currencies = {}
    slim_tickers    = {}
    fetch_times     = {}
    failed_ids      = set()

    logger("--- Phase 1: Loading exchanges ---")

    for ex_id in all_ex_ids:
        name = EXCHANGE_NAMES.get(ex_id, ex_id)

        try:
            opts = {"enableRateLimit": True, "timeout": 30000}
            opts.update(EXTRA_OPTS.get(ex_id, {}))
            ex        = getattr(ccxt, ex_id)(opts)
            ex.load_markets()
            mkt_count = len(ex.markets)

            raw = fetch_tickers_safe(ex, name, logger)

            symbol_map = {}
            for unified_sym, m in ex.markets.items():
                raw_id = m.get("id")
                if raw_id and raw_id != unified_sym:
                    symbol_map[raw_id] = unified_sym

            slim_m, slim_c         = strip_markets(ex)
            slim_markets[ex_id]    = slim_m
            slim_currencies[ex_id] = slim_c
            logger(f"Loaded {name} ({mkt_count} markets -> stripped)")

            if raw:
                fetch_times[ex_id]  = int(time.time() * 1000)
                slim_tickers[ex_id] = strip_tickers(raw, symbol_map)
                logger(f"Tickers {name} ({len(slim_tickers[ex_id])} -> stripped)")
            else:
                logger(f"No tickers for {name}")
                failed_ids.add(ex_id)
        except Exception as e:
            logger(f"Skipped {name}: {str(e)[:80]}")
            failed_ids.add(ex_id)

        try:
            del ex
        except NameError:
            pass
        gc.collect()

    loaded = [e for e in all_ex_ids if e not in failed_ids]
    logger(f"Phase 1 complete: {len(loaded)}/{len(all_ex_ids)} exchanges loaded")

    if len(loaded) < 2:
        logger("Need at least 2 loaded exchanges")
        return []

    # ── COINGECKO PRE-FETCH ───────────────────────────────────────────────────
    # Collect every unique base coin that appears in at least one valid combo's
    # common symbol set. Pre-populating the CG cache here means Phase 2 never
    # makes a live HTTP request — it only reads from in-memory dicts.
    logger("--- CoinGecko: collecting coins for pre-fetch ---")
    coins_to_prefetch = set()
    for b_id, s_id in combos:
        if b_id in failed_ids or s_id in failed_ids:
            continue
        b_slim_m = slim_markets.get(b_id, {})
        s_slim_m = slim_markets.get(s_id, {})
        if not b_slim_m or not s_slim_m:
            continue
        common = set(b_slim_m.keys()) & set(s_slim_m.keys())
        for sym in common:
            if symbol_ok(sym, b_slim_m) and symbol_ok(sym, s_slim_m):
                base, _ = parse_symbol(sym)
                if base:
                    coins_to_prefetch.add(base)

    logger(f"CoinGecko: {len(coins_to_prefetch)} unique coins to validate")
    prefetch_cg_networks_bulk(coins_to_prefetch, logger)

    # ── PHASE 2 ───────────────────────────────────────────────────────────────
    logger("--- Phase 2: Processing combos ---")

    all_results  = []
    current_keys = []

    for combo_num, (b_id, s_id) in enumerate(combos, 1):
        if b_id in failed_ids or s_id in failed_ids:
            continue

        b_name   = EXCHANGE_NAMES.get(b_id, b_id)
        s_name   = EXCHANGE_NAMES.get(s_id, s_id)
        b_slim_m = slim_markets.get(b_id, {})
        s_slim_m = slim_markets.get(s_id, {})
        b_slim_c = slim_currencies.get(b_id, {})
        s_slim_c = slim_currencies.get(s_id, {})
        b_tk     = slim_tickers.get(b_id, {})
        s_tk     = slim_tickers.get(s_id, {})

        if not b_slim_m or not s_slim_m or not b_tk or not s_tk:
            continue

        common  = set(b_slim_m.keys()) & set(s_slim_m.keys())
        symbols = [s for s in common if symbol_ok(s, b_slim_m) and symbol_ok(s, s_slim_m)]
        if not symbols:
            continue

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

        debug_this = (b_id == "mexc" or s_id == "mexc")
        dbg = {"no_ticker": 0, "stale": 0, "no_price": 0, "price_sanity": 0,
               "profit": 0, "volume": 0, "chain": 0, "passed": 0}

        for sym in symbols:
            bt  = b_tk.get(sym)
            st_ = s_tk.get(sym)
            if not bt or not st_:
                if debug_this: dbg["no_ticker"] += 1
                continue
            b_ref_ms = fetch_times.get(b_id)
            s_ref_ms = fetch_times.get(s_id)
            if not is_ticker_fresh(bt, b_ref_ms) or not is_ticker_fresh(st_, s_ref_ms):
                if debug_this: dbg["stale"] += 1
                continue

            bp = market_price_from_ticker(bt)
            sp = market_price_from_ticker(st_)
            if not bp or not sp:
                if debug_this: dbg["no_price"] += 1
                continue
            if abs(sp - bp) / bp > 0.5:
                if debug_this: dbg["price_sanity"] += 1
                continue

            b_fee  = b_slim_m.get(sym, {}).get("taker", 0.001)
            s_fee  = s_slim_m.get(sym, {}).get("taker", 0.001)
            spread = (sp - bp) / bp * 100
            profit = spread - (b_fee * 100 + s_fee * 100)
            if profit < min_p or profit > max_p:
                if debug_this: dbg["profit"] += 1
                continue

            b_vol = safe_usd_volume(b_id, sym, bt, bp, b_tk)
            s_vol = safe_usd_volume(s_id, sym, st_, sp, s_tk)
            if b_vol < min_vol or s_vol < min_vol:
                if debug_this: dbg["volume"] += 1
                continue

            base, quote = parse_symbol(sym)
            chain, w_ok, d_ok = choose_common_chain(
                b_slim_c, s_slim_c, base, exclude, include_all
            )

            if w_ok == "no" or d_ok == "no":
                if debug_this: dbg["chain"] += 1
                continue
            if not include_all and chain in ("No chain", "Unknown"):
                if debug_this: dbg["chain"] += 1
                continue

            if debug_this: dbg["passed"] += 1

            key = f"{sym}|{b_id}>{s_id}"
            current_keys.append(key)
            obs, exp = stability_and_expiry(key, profit)

            combo_results.append({
                "Pair":                sym,
                "Quote":               quote,
                "Buy@":                b_name,
                "Buy Price":           round(bp, 10),
                "Sell@":               s_name,
                "Sell Price":          round(sp, 10),
                "Spread %":            round(spread, 4),
                "Profit % After Fees": round(profit, 4),
                "Buy Vol (24h)":       fmt_usd(b_vol),
                "Sell Vol (24h)":      fmt_usd(s_vol),
                "Withdraw?":           w_ok,
                "Deposit?":            d_ok,
                "Blockchain":          chain,
                "Stability":           obs,
                "Est. Expiry":         exp,
            })

        all_results.extend(combo_results)
        logger(f"Combo {combo_num}/{len(combos)} {b_name}->{s_name}: {len(combo_results)} opportunities")
        if debug_this and sum(dbg.values()) > 0:
            logger(f"  MEXC debug ({b_name}->{s_name}): symbols={len(symbols)} "
                   f"no_ticker={dbg['no_ticker']} stale={dbg['stale']} "
                   f"no_price={dbg['no_price']} price_sanity={dbg['price_sanity']} "
                   f"profit={dbg['profit']} volume={dbg['volume']} "
                   f"chain={dbg['chain']} passed={dbg['passed']}")

    update_lifetime_for_disappeared(current_keys)

    slim_markets.clear()
    slim_currencies.clear()
    slim_tickers.clear()
    gc.collect()

    all_results.sort(key=lambda x: x["Profit % After Fees"], reverse=True)
    logger(f"Scan complete -- {len(all_results)} total opportunities")
    return all_results


# ====================== ROUTES ======================
@app.route('/')
def index():
    base_dir  = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, 'frontend.html')
    try:
        with open(html_path, 'r') as f:
            html = f.read()
    except FileNotFoundError:
        return "frontend.html not found. Please ensure it is in the same directory as backend.py.", 404
    except Exception as e:
        return f"Error loading frontend: {e}", 500
    return render_template_string(html, TOP_EXCHANGES=TOP_EXCHANGES, EXCHANGE_NAMES=EXCHANGE_NAMES)

@app.route('/api/scan', methods=['POST'])
def api_scan():
    if not _scan_lock.acquire(blocking=False):
        return jsonify({"error": "A scan is already in progress. Please wait."}), 429

    try:
        settings = request.get_json() or {}
        logs     = []

        def logger(msg):
            ts   = time.strftime("%H:%M:%S")
            line = f"[{ts}] {msg}"
            print(line, flush=True)
            logs.append(line)

        results  = run_scan(settings, logger)

        buy_ids  = settings.get("buy_exchanges", [])
        sell_ids = settings.get("sell_exchanges", [])
        if buy_ids and sell_ids and results:
            save_settings(settings)

        return jsonify({"results": results, "logs": logs})
    finally:
        _scan_lock.release()

@app.route('/api/exchanges')
def get_exchanges():
    return jsonify({
        "exchanges": TOP_EXCHANGES,
        "names":     EXCHANGE_NAMES
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
