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
    "mexc", "bitfinex", "bitget", "gateio", "cryptocom",
    "upbit", "whitebit", "poloniex", "bingx", "lbank",
    "bitstamp", "gemini", "bitrue", "xt", "huobi", "bitmart", "coinex",
    "toobit", "bullish"
]

EXCHANGE_NAMES = {
    "binance": "Binance", "okx": "OKX", "coinbase": "Coinbase",
    "kraken": "Kraken", "bybit": "Bybit", "kucoin": "KuCoin",
    "mexc": "MEXC", "bitfinex": "Bitfinex", "bitget": "Bitget",
    "gateio": "Gate.io", "cryptocom": "Crypto.com", "upbit": "Upbit",
    "whitebit": "WhiteBIT", "poloniex": "Poloniex", "bingx": "BingX",
    "lbank": "LBank", "bitstamp": "Bitstamp", "gemini": "Gemini",
    "bitrue": "Bitrue", "xt": "XT", "huobi": "Huobi",
    "bitmart": "BitMart", "coinex": "CoinEx",
    "toobit": "Toobit", "bullish": "Bullish"
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
    "toobit":   {"options": {"defaultType": "spot"}},
    # bullish is spot-only by default — no extra opts needed
    # Fix: Gemini crashes on load_markets without sandbox:False — ccxt uses
    # None as default base URL which causes NoneType + str concatenation error.
    "gemini":   {"sandbox": False},
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

# ====================== EXCHANGE API KEY HELPERS ======================
#
# Binance, Bybit and MEXC require API keys for ccxt to return full
# currencies/networks data (withdraw/deposit status per chain).
# Keys are read from Render environment variables — never hardcoded.
# BingX will show "Unverified" until keys are available for it.
#
# Required Render env vars:
#   BINANCE_API_KEY / BINANCE_API_SECRET
#   BYBIT_API_KEY   / BYBIT_API_SECRET
#   MEXC_API_KEY    / MEXC_API_SECRET

# Exchanges that need keys to expose network data via ccxt
_KEYED_EXCHANGES = {"binance", "bybit", "mexc"}

def get_api_keys(ex_id: str) -> dict:
    """
    Return {"apiKey": ..., "secret": ...} for exchanges that have keys
    configured in environment variables, or {} if not set.
    """
    prefix = ex_id.upper()
    api_key = os.environ.get(f"{prefix}_API_KEY", "")
    secret  = os.environ.get(f"{prefix}_API_SECRET", "")
    if api_key and secret:
        return {"apiKey": api_key, "secret": secret}
    return {}


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

        # Fall back to empty for exchanges without API keys (e.g. BingX).
        # Binance, Bybit and MEXC now have real network data via ccxt keys.
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
            # Fix 4: use `or 0.001` instead of default arg so explicit None
            # values (which some exchanges store) are also replaced with 0.001.
            "taker":  m.get("taker") or 0.001,
        }
        for sym, m in ex.markets.items()
    }
    # Fix 3: Bitfinex returns ex.currencies as a list instead of a dict.
    # Convert a list to a keyed dict using the coin symbol; dicts pass through.
    raw_curr = ex.currencies or {}
    if isinstance(raw_curr, list):
        raw_curr = {c.get("id", c.get("code", "")): c for c in raw_curr if c}
    slim_currencies = {
        coin: {"networks": (c.get("networks") or {}) if c else {}}
        for coin, c in raw_curr.items()
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
            # Inject API keys for exchanges that need them to expose
            # currency/network data. Keys come from Render env vars.
            keys = get_api_keys(ex_id)
            if keys:
                opts.update(keys)
                logger(f"Using API keys for {name}")
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
