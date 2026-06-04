import requests
import time
import json
import os
import threading
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import Counter

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("tsm_v32.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8778362544:AAG3Pdr98EySWSpsPLvlM10qUb7TeTPc-u4")
CHAT_ID        = os.getenv("CHAT_ID", "8005940008")
NEWS_API_KEY   = os.getenv("NEWS_API_KEY", "")

BINANCE_PRICE_URL   = "https://data-api.binance.vision/api/v3/ticker/price"
BINANCE_KLINE_URL   = "https://data-api.binance.vision/api/v3/klines"
BINANCE_AGG_URL     = "https://api.binance.com/api/v3/aggTrades"
BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
BINANCE_OI_URL      = "https://fapi.binance.com/futures/data/openInterestHist"

trade_lock = threading.Lock()
IST        = ZoneInfo("Asia/Kolkata")

# ================= COINS =================
COINS = list(dict.fromkeys([
    "BTC","ETH","BNB","SOL","XRP","DOGE","ADA","TRX","AVAX","SHIB",
    "DOT","LINK","BCH","NEAR","LTC","UNI","APT","ETC","HBAR","FIL",
    "ARB","VET","INJ","OP","ATOM","TIA","SUI","SEI","ALGO","EGLD",
    "FLOW","EOS","XTZ","AAVE","MKR","GRT","SNX","COMP","CRV","SUSHI",
    "LDO","CAKE","1INCH","DYDX","GMX","ENS","PENDLE","RNDR","FET","WLD",
    "AR","THETA","LPT","AKT","SAND","MANA","AXS","GALA","CHZ","APE",
    "GMT","ENJ","PEPE","WIF","FLOKI","BONK","ORDI","BOME","NOT","DOGS",
    "JUP","PYTH","JTO","STRK","EIGEN","ETHFI","IO","ZERO","ONDO",
    "BLUR","CFX","METIS","MANTA","ZETA","TRB","ALT","PIXEL","PORTAL","STPT","KAS"
]))

# ================= STATE =================
active_trades              = {}
pending_signals            = {}
hourly_queue               = {}
sent_coins                 = []
daily_losses               = 0
circuit_breaker_until      = None
last_reset_day             = datetime.now(IST).date()
trade_journal              = []
learning_notes             = []
daily_sent_coins           = set()
consecutive_loss_patterns  = {}
price_alerts               = {}
coin_cooldowns             = {}   # {coin: datetime} — cooldown after loss
market_memory = {
    "bull":     {"wins": 0, "losses": 0, "best_pattern": None},
    "bear":     {"wins": 0, "losses": 0, "best_pattern": None},
    "sideways": {"wins": 0, "losses": 0, "best_pattern": None}
}
pattern_stats = {p: {"signals": 0, "wins": 0, "losses": 0, "total_pnl": 0.0,
                      "weight": 1.0,  # adaptive weight — updates after every trade
                      "bull_wr": 0.0, "bear_wr": 0.0, "sideways_wr": 0.0
                      } for p in [
    "EMA Trend","Breakout","Pullback to 20 EMA","RSI Reversal","Momentum Surge",
    "Volume Spike","Double Bottom","Double Top","Support Bounce","Resistance Rejection",
    "Bullish Engulfing","Bearish Engulfing","Volume Breakout","Bull Flag Break","Bear Flag Break"
]}

last_update_id         = None
last_batch_time        = 0
last_river_time        = 0
last_hourly_time       = time.time()
last_pnl_update_time   = time.time() + 1800
last_weekly_report_day = None

# ================= CONSTANTS =================
SCAN_INTERVAL            = 300
BATCH_INTERVAL           = 1800
RIVER_INTERVAL           = 900
MIN_SETUP_SCORE          = 94
MIN_PRIMARY_SCORE        = 82
INSTANT_SIGNAL_THRESHOLD = 97
MIN_PROFIT_TARGET        = 15.0
DELAY_BETWEEN_COINS      = 0.15
MAX_SIGNALS_PER_BATCH    = 1
MAX_ACTIVE_TRADES        = 5
SIGNAL_EXPIRY_MINUTES    = 30
INSTANT_EXPIRY_MINUTES   = 15
ATR_SL_MULTIPLIER        = 2.5
ATR_TP_MULTIPLIER        = 5.0
MAX_DAILY_LOSSES         = 3
CIRCUIT_BREAKER_MIN_LOSS = -5.0
WHALE_TRADE_THRESHOLD    = 500000
ATR_VOLATILITY_RATIO     = 3.0
CONSEC_LOSS_SUSPEND      = 5
MIN_SIGNALS_TO_SUSPEND   = 15
SUSPEND_HOURS            = 12
ADX_MIN_TREND             = 21
ST_PERIOD                = 10
ST_MULTIPLIER            = 3.0
MIN_SL_PCT               = 0.02
DEAD_HOUR_START          = 2
DEAD_HOUR_END            = 7
BTC_CORRELATED           = ["ETH","BNB","SOL","AVAX","NEAR","APT","SUI"]
LEV_TIER_1               = ["BTC","ETH"]
LEV_TIER_2               = ["BNB","SOL","XRP","ADA","AVAX","DOT","LINK","LTC"]
LEV_TIER_3               = ["DOGE","SHIB","PEPE","WIF","FLOKI","BONK","DOGS",
                             "BOME","NOT","APE","GMT","CHZ","GALA","SAND","MANA"]

BOT_VERSION = "v32"
BOT_NAME    = "TRADING SIGNAL MASTER"
BOT_HEADER  = f"⚙️ {BOT_NAME} {BOT_VERSION}"
STARTUP_MSG = (
    "╔══════════════════════════════════════╗\n"
    "║   🚀  TRADING SIGNAL MASTER  v32  🚀  ║\n"
    "║   Smart  •  Fast  •  Accurate  •  AI ║\n"
    "╚══════════════════════════════════════╝"
)

def S(c="─", n=30): return c * n
def fmt_pnl(v): return ("🟢 " if v >= 0 else "🔴 ") + f"{v:+.2f}%"

# ================= PERSISTENCE =================
def save_active_trades():
    with trade_lock:
        try:
            s = {k: {**v, "timestamp": v["timestamp"].isoformat(),
                     "expires_at": v["expires_at"].isoformat() if v.get("expires_at") else None}
                 for k, v in active_trades.items()}
            with open("active_trades.json", "w") as f: json.dump(s, f)
        except Exception as e: logger.error(f"save_active_trades: {e}")

def load_active_trades():
    global active_trades
    try:
        if os.path.exists("active_trades.json"):
            with open("active_trades.json") as f: data = json.load(f)
            active_trades = {k: {**v,
                "timestamp":  datetime.fromisoformat(v["timestamp"]),
                "expires_at": datetime.fromisoformat(v["expires_at"]) if v.get("expires_at") else None}
                for k, v in data.items()}
            logger.info(f"Loaded {len(active_trades)} active trades.")
    except Exception as e: logger.error(f"load_active_trades: {e}")

def save_trade_history():
    with trade_lock:
        try:
            with open("trades.json", "w") as f: json.dump(pattern_stats, f)
        except Exception as e: logger.error(f"save_trade_history: {e}")

def load_trade_history():
    global pattern_stats
    try:
        if os.path.exists("trades.json"):
            with open("trades.json") as f: loaded = json.load(f)
            for p in pattern_stats:
                if p in loaded: pattern_stats[p] = loaded[p]
    except Exception as e: logger.error(f"load_trade_history: {e}")

def save_journal():
    try:
        with open("journal.json", "w") as f: json.dump(trade_journal, f)
    except Exception as e: logger.error(f"save_journal: {e}")

def load_journal():
    global trade_journal
    try:
        if os.path.exists("journal.json"):
            with open("journal.json") as f: trade_journal = json.load(f)
        logger.info(f"Loaded {len(trade_journal)} journal entries.")
    except Exception as e: logger.error(f"load_journal: {e}")

def save_learning():
    try:
        with open("learning.json", "w") as f:
            json.dump({"notes": learning_notes, "memory": market_memory,
                       "clp": consecutive_loss_patterns}, f)
    except Exception as e: logger.error(f"save_learning: {e}")

def load_learning():
    global learning_notes, market_memory, consecutive_loss_patterns
    try:
        if os.path.exists("learning.json"):
            with open("learning.json") as f: data = json.load(f)
            learning_notes            = data.get("notes", [])
            market_memory.update(data.get("memory", {}))
            consecutive_loss_patterns = data.get("clp", {})
    except Exception as e: logger.error(f"load_learning: {e}")

def save_daily_sent():
    try:
        with open("daily_sent.json", "w") as f:
            json.dump({"date": str(datetime.now(IST).date()), "coins": list(daily_sent_coins)}, f)
    except Exception as e: logger.error(f"save_daily_sent: {e}")

def load_daily_sent():
    global daily_sent_coins
    try:
        if os.path.exists("daily_sent.json"):
            with open("daily_sent.json") as f: data = json.load(f)
            if data.get("date") == str(datetime.now(IST).date()):
                daily_sent_coins = set(data.get("coins", []))
    except Exception as e: logger.error(f"load_daily_sent: {e}")

def save_alerts():
    try:
        with open("alerts.json", "w") as f: json.dump(price_alerts, f)
    except Exception as e: logger.error(f"save_alerts: {e}")

def load_alerts():
    global price_alerts
    try:
        if os.path.exists("alerts.json"):
            with open("alerts.json") as f: price_alerts = json.load(f)
    except Exception as e: logger.error(f"load_alerts: {e}")

def save_circuit_breaker():
    try:
        with open("circuit_breaker.json", "w") as f:
            json.dump({"daily_losses": daily_losses,
                       "circuit_breaker_until": circuit_breaker_until,
                       "date": str(last_reset_day)}, f)
    except Exception as e: logger.error(f"save_circuit_breaker: {e}")

def load_circuit_breaker():
    global daily_losses, circuit_breaker_until, last_reset_day
    try:
        if os.path.exists("circuit_breaker.json"):
            with open("circuit_breaker.json") as f: data = json.load(f)
            saved_date = data.get("date", "")
            today      = str(datetime.now(IST).date())
            if saved_date == today:
                daily_losses           = data.get("daily_losses", 0)
                circuit_breaker_until  = data.get("circuit_breaker_until")
                last_reset_day         = datetime.now(IST).date()
                logger.info(f"Circuit breaker loaded: {daily_losses} losses, until: {circuit_breaker_until}")
            else:
                daily_losses          = 0
                circuit_breaker_until = None
                logger.info("Circuit breaker reset — new day.")
    except Exception as e: logger.error(f"load_circuit_breaker: {e}")

# ================= UTILS =================
def format_price(p):
    if p >= 1000:    return f"{p:.2f}"
    elif p >= 1:     return f"{p:.4f}"
    elif p >= 0.01:  return f"{p:.6f}"
    else:            return f"{p:.8f}"

def get_ist_time():     return datetime.now(IST).strftime("%I:%M:%S %p IST")
def get_ist_datetime(): return datetime.now(IST)

# ================= TELEGRAM =================
def send_telegram(text, parse_mode="HTML", reply_markup=None):
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode}
    if reply_markup: payload["reply_markup"] = reply_markup
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=payload, timeout=15)
        if res.status_code != 200:
            logger.warning(f"Telegram [{res.status_code}]: {res.text[:100]}")
        return res.status_code == 200
    except requests.RequestException as e:
        logger.error(f"Telegram error: {e}"); return False

# ================= BINANCE =================
def get_price(symbol):
    try:
        res = requests.get(BINANCE_PRICE_URL, params={"symbol": symbol}, timeout=10)
        return float(res.json()["price"]) if res.status_code == 200 else None
    except Exception as e:
        logger.warning(f"get_price {symbol}: {e}"); return None

def get_klines(symbol, interval, limit=100):
    try:
        res = requests.get(BINANCE_KLINE_URL,
                           params={"symbol": symbol, "interval": interval, "limit": limit},
                           timeout=10)
        return res.json() if res.status_code == 200 else []
    except Exception as e:
        logger.warning(f"get_klines {symbol}: {e}"); return []

# ================= INDICATORS =================
def calculate_ema(closes, period):
    if len(closes) < period: return None
    ema = sum(closes[:period]) / period
    k   = 2.0 / (period + 1)
    for p in closes[period:]: ema = p * k + ema * (1 - k)
    return ema

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(0, d)); losses.append(max(0, -d))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    return 100.0 - (100.0 / (1 + ag / al)) if al != 0 else 100.0

def calculate_atr(klines, period=14):
    if len(klines) < period + 1: return 0.0
    trs = []
    for i in range(1, len(klines)):
        h = float(klines[i][2]); l = float(klines[i][3]); pc = float(klines[i-1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period

def calculate_adx(klines, period=14):
    if len(klines) < period * 2 + 1: return 30.0
    try:
        highs  = [float(k[2]) for k in klines]
        lows   = [float(k[3]) for k in klines]
        closes = [float(k[4]) for k in klines]
        pdm, mdm, trl = [], [], []
        for i in range(1, len(klines)):
            hd = highs[i] - highs[i-1]; ld = lows[i-1] - lows[i]
            pdm.append(hd if hd > ld and hd > 0 else 0)
            mdm.append(ld if ld > hd and ld > 0 else 0)
            tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
            trl.append(tr)
        def smooth(data, p):
            s = sum(data[:p]); r = [s]
            for v in data[p:]: s = s - s/p + v; r.append(s)
            return r
        atr_s = smooth(trl, period); pdm_s = smooth(pdm, period); mdm_s = smooth(mdm, period)
        pdi = [100*p/a if a else 0 for p, a in zip(pdm_s, atr_s)]
        mdi = [100*m/a if a else 0 for m, a in zip(mdm_s, atr_s)]
        dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
        return sum(dx[-period:]) / period if len(dx) >= period else 30.0
    except Exception: return 30.0

def calculate_supertrend(klines, period=10, multiplier=3.0):
    if len(klines) < period + 1: return None
    try:
        highs  = [float(k[2]) for k in klines]
        lows   = [float(k[3]) for k in klines]
        closes = [float(k[4]) for k in klines]
        atr    = calculate_atr(klines, period)
        hl2    = (highs[-1] + lows[-1]) / 2
        upper  = hl2 + multiplier * atr
        lower  = hl2 - multiplier * atr
        price  = closes[-1]
        prev   = closes[-2] if len(closes) > 1 else price
        if price > lower and prev > lower:   return "BUY"
        if price < upper and prev < upper:   return "SELL"
        return "BUY" if price > hl2 else "SELL"
    except Exception: return None

def calculate_vwap(klines):
    try:
        total_pv  = sum(((float(k[2])+float(k[3])+float(k[4]))/3)*float(k[5]) for k in klines)
        total_vol = sum(float(k[5]) for k in klines)
        return total_pv / total_vol if total_vol > 0 else None
    except Exception: return None

def detect_rsi_divergence(closes):
    if len(closes) < 10: return None
    try:
        prices   = closes[-6:]
        rsi_vals = [calculate_rsi(closes[:i+1]) for i in range(len(closes)-6, len(closes))]
        if prices[-1] < prices[0] and rsi_vals[-1] > rsi_vals[0]: return "BULLISH_DIV"
        if prices[-1] > prices[0] and rsi_vals[-1] < rsi_vals[0]: return "BEARISH_DIV"
        return None
    except Exception: return None

def detect_supply_demand_zones(klines):
    zones = {"demand": [], "supply": []}
    try:
        closes = [float(k[4]) for k in klines]; opens = [float(k[1]) for k in klines]
        highs  = [float(k[2]) for k in klines]; lows  = [float(k[3]) for k in klines]
        for i in range(3, len(klines)-1):
            body     = abs(closes[i] - opens[i])
            avg_body = sum(abs(closes[j]-opens[j]) for j in range(i-3, i)) / 3
            if avg_body == 0: continue
            if closes[i] > opens[i] and body > avg_body * 1.5:
                zones["demand"].append({"high": max(opens[i], closes[i-1]),
                                        "low":  min(lows[i-1], lows[i-2])})
            if closes[i] < opens[i] and body > avg_body * 1.5:
                zones["supply"].append({"high": max(highs[i-1], highs[i-2]),
                                        "low":  min(opens[i], closes[i-1])})
    except Exception: pass
    return zones

def is_in_zone(price, direction, zones):
    key = "demand" if direction == "BUY" else "supply"
    for zone in zones.get(key, [])[-5:]:
        if zone["low"] * 0.995 <= price <= zone["high"] * 1.005:
            return True, f"{format_price(zone['low'])}-{format_price(zone['high'])}"
    return False, ""

def is_candle_strong(klines):
    try:
        last  = klines[-1]
        body  = abs(float(last[4]) - float(last[1]))
        total = float(last[2]) - float(last[3])
        return (body / total) >= 0.45 if total > 0 else False
    except Exception: return True

# ================= MARKET CONDITION =================
def detect_market_condition(btc_price, btc_klines):
    try:
        closes    = [float(k[4]) for k in btc_klines]
        ema20     = calculate_ema(closes, 20); ema50 = calculate_ema(closes, 50)
        high_20   = max(closes[-20:]); low_20 = min(closes[-20:])
        range_pct = ((high_20 - low_20) / low_20) * 100 if low_20 > 0 else 0
        if ema20 and ema50:
            if ema20 > ema50 * 1.02 and btc_price > ema20:   return "bull"
            elif ema20 < ema50 * 0.98 and btc_price < ema20: return "bear"
        return "sideways" if range_pct < 5.0 else ("bull" if btc_price > (ema50 or btc_price) else "bear")
    except Exception: return "sideways"

# ================= CIRCUIT BREAKER =================
def check_circuit_breaker():
    global daily_losses, circuit_breaker_until, last_reset_day
    today = datetime.now(IST).date()
    # Reset at midnight every day
    if today != last_reset_day:
        daily_losses          = 0
        circuit_breaker_until = None
        last_reset_day        = today
        save_circuit_breaker()
        logger.info("Circuit breaker reset — new day.")
        return False
    # Check if circuit breaker time has passed
    if circuit_breaker_until:
        try:
            until_dt = datetime.fromisoformat(circuit_breaker_until)
            if datetime.now(IST) >= until_dt:
                circuit_breaker_until = None
                daily_losses          = 0
                save_circuit_breaker()
                send_telegram(
                    f"✅ <b>{BOT_HEADER}</b>\n"
                    f"{S()}\n"
                    f"🟢 Circuit Breaker RESET\n"
                    f"Bot is now scanning for signals again.\n"
                    f"{S()}"
                )
                logger.info("Circuit breaker lifted — resuming signals.")
                return False
            return True
        except Exception:
            circuit_breaker_until = None
            return False
    return daily_losses >= MAX_DAILY_LOSSES

def increment_daily_losses(pnl):
    global daily_losses, circuit_breaker_until
    if pnl > CIRCUIT_BREAKER_MIN_LOSS:
        logger.info(f"Small loss {pnl:.2f}% — not counted toward circuit breaker")
        return
    daily_losses += 1
    logger.warning(f"Circuit breaker count: {daily_losses}/{MAX_DAILY_LOSSES} | PnL: {pnl:.2f}%")
    if daily_losses >= MAX_DAILY_LOSSES:
        # Set circuit breaker until midnight IST
        now      = datetime.now(IST)
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        circuit_breaker_until = midnight.isoformat()
        save_circuit_breaker()
        send_telegram(
            f"🚨 <b>{BOT_HEADER}</b>\n"
            f"{S('═')}\n"
            f"⛔ CIRCUIT BREAKER ACTIVE\n\n"
            f"3 significant losses today (each worse than -5%).\n"
            f"No more signals until midnight IST.\n\n"
            f"🔄 Auto-resumes at: 12:00 AM IST\n"
            f"Protect your capital. 🛡️\n"
            f"{S('═')}"
        )

# ================= SESSION FILTER =================
def is_good_trading_session():
    hour = datetime.now(IST).hour
    if DEAD_HOUR_START <= hour < DEAD_HOUR_END:
        logger.info(f"Dead session {hour}:xx IST — skipping")
        return False
    return True

# ================= SMART LEVERAGE =================
def get_smart_leverage(symbol, atr_pct, score):
    base = symbol.replace("USDT", "")
    if base in LEV_TIER_1:   base_lev, hard_cap = 10, 12
    elif base in LEV_TIER_2: base_lev, hard_cap = 6,  8
    elif base in LEV_TIER_3: base_lev, hard_cap = 2,  3
    elif atr_pct < 2.0:      base_lev, hard_cap = 5,  7
    elif atr_pct < 4.0:      base_lev, hard_cap = 4,  6
    else:                    base_lev, hard_cap = 3,  5
    bonus = 2 if score >= 99 else 1 if score >= 97 else 0
    return min(base_lev + bonus, hard_cap)

# ================= SIGNAL GRADE =================
def get_signal_grade(score, whale, oi_rising, tf_score, vol_ok, rsi_ok,
                     funding_ok, st_ok, vwap_ok, zone_ok, adx_val):
    pts = 0
    if score >= 98:    pts += 3
    elif score >= 96:  pts += 2
    else:              pts += 1
    if whale:          pts += 2
    if oi_rising:      pts += 2
    if tf_score == 3:  pts += 2
    elif tf_score == 2:pts += 1
    if vol_ok:         pts += 1
    if rsi_ok:         pts += 1
    if funding_ok:     pts += 1
    if st_ok:          pts += 2
    if vwap_ok:        pts += 1
    if zone_ok:        pts += 2
    if adx_val >= 35:  pts += 1
    if pts >= 14:      return "⭐⭐⭐ Grade A+"
    elif pts >= 11:    return "⭐⭐ Grade A"
    elif pts >= 8:     return "✅ Grade B"
    else:              return "⚠️ Grade C"

# ================= FILTERS =================
def is_volume_confirmed(klines):
    vols = [float(k[5]) for k in klines]
    return len(vols) >= 20 and vols[-1] > sum(vols[-20:]) / 20 * 1.05

def is_rsi_valid(closes, direction):
    rsi = calculate_rsi(closes)
    return not (direction == "BUY" and rsi > 72) and not (direction == "SELL" and rsi < 28)

def is_volatility_normal(klines):
    a_now = calculate_atr(klines, 14); a_slow = calculate_atr(klines, 50)
    return a_slow == 0 or (a_now / a_slow) <= ATR_VOLATILITY_RATIO

def is_pattern_blacklisted(name):
    s = pattern_stats.get(name)
    if not s or s["signals"] < 10: return False
    return (s["wins"] / s["signals"]) * 100 < 40

def is_pattern_suspended(name):
    d = consecutive_loss_patterns.get(name, {})
    if d.get("consecutive_losses", 0) >= CONSEC_LOSS_SUSPEND:
        su = d.get("suspended_until")
        if su:
            try:
                if datetime.now(IST) < datetime.fromisoformat(su): return True
                consecutive_loss_patterns[name]["consecutive_losses"] = 0
                consecutive_loss_patterns[name]["suspended_until"]    = None
            except Exception: pass
    return False

def too_many_correlated_active():
    return sum(1 for c in active_trades if c in BTC_CORRELATED) >= 2

def get_funding_rate(symbol):
    try:
        res = requests.get(BINANCE_FUNDING_URL, params={"symbol": symbol, "limit": 1}, timeout=10)
        return float(res.json()[0]["fundingRate"]) if res.status_code == 200 and res.json() else None
    except Exception as e:
        logger.warning(f"funding {symbol}: {e}"); return None

def is_funding_favorable(symbol, direction):
    rate = get_funding_rate(symbol)
    if rate is None: return True
    if direction == "BUY"  and rate >  0.002: return False
    if direction == "SELL" and rate < -0.002: return False
    return True

def get_oi_trend(symbol):
    try:
        res = requests.get(BINANCE_OI_URL,
                           params={"symbol": symbol, "period": "15m", "limit": 5}, timeout=10)
        if res.status_code == 200 and len(res.json()) >= 2:
            d = res.json()
            return float(d[-1]["sumOpenInterest"]) > float(d[-2]["sumOpenInterest"])
        return None
    except Exception as e:
        logger.warning(f"OI {symbol}: {e}"); return None

def has_whale_activity(symbol):
    try:
        res = requests.get(BINANCE_AGG_URL, params={"symbol": symbol, "limit": 20}, timeout=10)
        if res.status_code == 200:
            for t in res.json():
                if float(t["p"]) * float(t["q"]) > WHALE_TRADE_THRESHOLD: return True
        return False
    except Exception as e:
        logger.warning(f"whale {symbol}: {e}"); return False

def get_fear_greed_index():
    try:
        res = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        return int(res.json()["data"][0]["value"]) if res.status_code == 200 else 50
    except Exception as e:
        logger.warning(f"F&G: {e}"); return 50

def is_sentiment_valid(direction, fng):
    return not (direction == "BUY" and fng < 20) and not (direction == "SELL" and fng > 80)

def get_htf_trend(symbol, interval="1h"):
    try:
        klines = get_klines(symbol, interval, 50)
        if not klines or len(klines) < 50: return 0
        closes = [float(k[4]) for k in klines]
        e20 = calculate_ema(closes, 20); e50 = calculate_ema(closes, 50)
        if e20 and e50: return 1 if e20 > e50 else -1
        return 0
    except Exception as e:
        logger.warning(f"HTF {symbol} {interval}: {e}"); return 0

def get_timeframe_score(symbol, direction):
    di = 1 if direction == "BUY" else -1
    h4 = get_htf_trend(symbol, "4h"); h1 = get_htf_trend(symbol, "1h")
    if h4 != 0 and h4 != di: return -1
    score = 0
    if h4 == di: score += 2
    if h1 == di: score += 1
    return score

def get_structure_sl(klines, direction, entry, atr):
    lows  = [float(k[3]) for k in klines[-20:]]
    highs = [float(k[2]) for k in klines[-20:]]
    min_dist = entry * MIN_SL_PCT
    if direction == "BUY":
        sl = min(min(lows) * 0.998, entry - atr * ATR_SL_MULTIPLIER)
        return min(sl, entry - min_dist)
    sl = max(max(highs) * 1.002, entry + atr * ATR_SL_MULTIPLIER)
    return max(sl, entry + min_dist)

# ================= LEARNING SYSTEM =================
def learn_from_trade(coin, pattern, result, pnl, mc, tf_score):
    global learning_notes, market_memory, consecutive_loss_patterns
    if result == "WIN": market_memory[mc]["wins"]   += 1
    else:               market_memory[mc]["losses"] += 1
    wins_by_pat = {}
    for e in trade_journal:
        if e.get("market_condition") == mc and e.get("result") == "WIN":
            p = e.get("pattern", "?"); wins_by_pat[p] = wins_by_pat.get(p, 0) + 1
    if wins_by_pat:
        market_memory[mc]["best_pattern"] = max(wins_by_pat, key=wins_by_pat.get)
    if pattern not in consecutive_loss_patterns:
        consecutive_loss_patterns[pattern] = {"consecutive_losses": 0, "suspended_until": None}
    if result == "LOSS":
        consecutive_loss_patterns[pattern]["consecutive_losses"] += 1
        cl   = consecutive_loss_patterns[pattern]["consecutive_losses"]
        sigs = pattern_stats.get(pattern, {}).get("signals", 0)
        if cl >= CONSEC_LOSS_SUSPEND and sigs >= MIN_SIGNALS_TO_SUSPEND:
            su = (datetime.now(IST) + timedelta(hours=SUSPEND_HOURS)).isoformat()
            consecutive_loss_patterns[pattern]["suspended_until"] = su
            send_telegram(
                f"🧠 <b>{BOT_HEADER} Pattern Suspended</b>\n{S()}\n"
                f"⚠️ <b>{pattern}</b>\n"
                f"{cl} consecutive losses ({sigs} total signals)\n"
                f"Suspended {SUSPEND_HOURS}h automatically 🔒\n{S()}"
            )
    else:
        consecutive_loss_patterns[pattern]["consecutive_losses"] = 0
        consecutive_loss_patterns[pattern]["suspended_until"]    = None
    # Adaptive weight update — ML-like self-learning
    if pattern in pattern_stats:
        s    = pattern_stats[pattern]
        sigs = s.get("signals", 0)
        if sigs >= 3:
            wr = (s["wins"] / sigs) * 100
            # Update weight based on win rate
            if wr >= 70:   s["weight"] = min(s["weight"] + 0.1, 1.5)   # boost good patterns
            elif wr < 40:  s["weight"] = max(s["weight"] - 0.15, 0.5)  # reduce bad patterns
            # Update market condition win rates
            mc_trades = [t for t in trade_journal if t.get("pattern") == pattern and t.get("market_condition") == mc]
            mc_wins   = sum(1 for t in mc_trades if t["result"] == "WIN")
            mc_wr     = (mc_wins / len(mc_trades) * 100) if mc_trades else 50.0
            s[f"{mc}_wr"] = round(mc_wr, 1)
            logger.info(f"Pattern weight update: {pattern} weight={s['weight']:.2f} wr={wr:.1f}%")

    stats = pattern_stats.get(pattern, {}); sigs = stats.get("signals", 0); note = None
    if sigs >= 5:
        wr = (stats["wins"] / sigs) * 100
        if result == "LOSS" and wr < 45:
            note = f"⚠️ '{pattern}' only {wr:.1f}% WR after {sigs} signals in {mc} market."
        elif result == "WIN" and wr > 70:
            note = f"✅ '{pattern}' strong — {wr:.1f}% WR in {mc} market."
        elif result == "LOSS" and mc == "sideways":
            note = f"📊 Loss on '{pattern}' in sideways. Reduce size when BTC ranges."
        elif tf_score < 3 and result == "LOSS":
            note = f"📉 Loss on {coin} TF score {tf_score}/3. Need full TF alignment."
    if note and note not in learning_notes:
        learning_notes.append(note)
        if len(learning_notes) > 100: learning_notes = learning_notes[-100:]
        if "⚠️" in note or "📉" in note:
            send_telegram(f"🧠 <b>{BOT_HEADER} Auto Learning Alert</b>\n{S()}\n{note}\n{S()}")
    save_learning()


# ================= ADAPTIVE PATTERN SCORING =================
def get_adjusted_score(pattern_name, base_score, market_condition):
    """
    Replaces hardcoded base scores with live performance-adjusted scores.
    Patterns earn their rank through real trade results.
    After enough data, a low-base pattern with high win rate will
    naturally outscore a high-base pattern that keeps losing.
    """
    stats   = pattern_stats.get(pattern_name, {})
    signals = stats.get("signals", 0)
    if signals < 5:
        return base_score  # not enough data yet — trust base score

    overall_wr = (stats["wins"] / signals) * 100
    mc_wr      = stats.get(f"{market_condition}_wr", overall_wr)
    weight     = stats.get("weight", 1.0)

    # Blend real performance with base score
    # More trades = more weight given to real performance
    if signals >= 20:   pf = 0.6
    elif signals >= 10: pf = 0.4
    else:               pf = 0.2

    adjusted = (base_score * (1 - pf) + mc_wr * pf) * weight
    return min(round(adjusted, 1), 99.0)

def get_all_pattern_scores(patterns, market_condition):
    """
    Returns all detected patterns with their adjusted scores.
    No pattern is discarded — all are ranked by live performance.
    BUY and SELL patterns are separated so we pick best of each direction.
    """
    scored = []
    for name, base_score, direction in patterns:
        adj_score = get_adjusted_score(name, base_score, market_condition)
        scored.append((name, adj_score, direction, base_score))
    # Sort by adjusted score descending
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored

def generate_weekly_insight():
    today = datetime.now(IST).date()
    wt = [j for j in trade_journal
          if (today - datetime.strptime(j["date"], "%Y-%m-%d").date()).days < 7]
    if not wt: return "Not enough data for weekly insight yet."
    wins = [t for t in wt if t["result"] == "WIN"]
    losses = [t for t in wt if t["result"] == "LOSS"]
    total = len(wt); wr = (len(wins) / total * 100) if total > 0 else 0
    day_wins = {}
    for t in wins: d = t["date"]; day_wins[d] = day_wins.get(d, 0) + 1
    best_day  = max(day_wins, key=day_wins.get) if day_wins else None
    wp        = [t["pattern"] for t in wins]; lp = [t["pattern"] for t in losses]
    best_pat  = Counter(wp).most_common(1)[0][0]  if wp else None
    worst_pat = Counter(lp).most_common(1)[0][0]  if lp else None
    sw_losses = sum(1 for t in losses if t.get("market_condition") == "sideways")
    msg  = f"🧠 <b>{BOT_HEADER} Weekly AI Insight</b>\n{S()}\n"
    msg += f"📊 {len(wins)}W / {len(losses)}L | {wr:.1f}% WR\n\n"
    if best_day:  msg += f"📅 Best day: {best_day}\n"
    if best_pat:  msg += f"⭐ Best pattern: {best_pat}\n"
    if worst_pat: msg += f"⚠️ Most losses from: {worst_pat}\n"
    if sw_losses >= 2: msg += f"📊 {sw_losses} losses in sideways — reduce size when BTC ranges\n"
    if wr >= 70:   msg += "\n🔥 Excellent week! Keep it up."
    elif wr >= 50: msg += "\n✅ Decent week. Stay disciplined."
    else:          msg += "\n⚠️ Tough week. Review learning notes."
    msg += f"\n{S()}"
    return msg

# ================= NEWS =================
def get_crypto_news():
    headlines = []
    try:
        res = requests.get(
            "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest",
            timeout=10)
        if res.status_code == 200:
            for a in res.json().get("Data", [])[:6]:
                title  = a.get("title", "")[:85]
                source = a.get("source_info", {}).get("name", "")
                if title: headlines.append(f"📰 <b>{title}</b>\n   <i>— {source}</i>")
    except Exception as e: logger.warning(f"CryptoCompare: {e}")
    if NEWS_API_KEY and not headlines:
        try:
            res = requests.get("https://cryptopanic.com/api/v1/posts/",
                               params={"auth_token": NEWS_API_KEY, "kind": "news", "filter": "hot"},
                               timeout=10)
            if res.status_code == 200:
                for item in res.json().get("results", [])[:5]:
                    headlines.append(f"📰 {item['title'][:85]}")
        except Exception as e: logger.warning(f"CryptoPanic: {e}")
    fng_line = ""
    try:
        res = requests.get("https://api.alternative.me/fng/?limit=3", timeout=10)
        if res.status_code == 200:
            data = res.json()["data"]; latest = data[0]
            yesterday = data[1] if len(data) > 1 else None
            fv = int(latest["value"]); fc = latest["value_classification"]; chg = ""
            if yesterday:
                diff = fv - int(yesterday["value"])
                chg  = f" ({'+' if diff >= 0 else ''}{diff} vs yesterday)"
            emoji = "😨" if fv <= 25 else "😟" if fv <= 45 else "😐" if fv <= 55 else "😊" if fv <= 75 else "🤑"
            fng_line = f"{emoji} <b>Fear & Greed: {fv} — {fc}</b>{chg}"
    except Exception as e: logger.warning(f"F&G news: {e}")
    prices = []
    try:
        pairs = [("BTCUSDT","₿  BTC"),("ETHUSDT","Ξ  ETH"),("SOLUSDT","◎  SOL"),
                 ("BNBUSDT","🔶 BNB"),("XRPUSDT","✦  XRP")]
        for sym, label in pairs:
            p = get_price(sym)
            if p:
                line = f"{label}  <code>${p:>12,.2f}</code>" if p >= 1 else f"{label}  <code>${p:>14,.6f}</code>"
                prices.append(line)
    except Exception as e: logger.warning(f"Price snap: {e}")
    msg  = f"📰 <b>{BOT_HEADER} Market Update</b>\n"
    msg += f"🕐 {get_ist_time()}\n{S()}\n\n"
    if fng_line: msg += fng_line + "\n\n"
    if prices:
        msg += "<b>💰 Live Prices:</b>\n"
        msg += "\n".join(prices) + "\n\n"
    if headlines:
        msg += f"{S()}\n<b>🗞️ Latest Crypto News:</b>\n\n"
        msg += "\n\n".join(headlines[:5])
    else:
        msg += "No news available right now."
    msg += f"\n\n{S()}\n🔄 Updated: {get_ist_time()}"
    return msg

# ================= BACKTESTING =================
def run_backtest(symbol):
    try:
        klines = get_klines(symbol, "15m", 1000)
        if not klines or len(klines) < 100:
            return f"Not enough data for {symbol}"
        results     = {"WIN": 0, "LOSS": 0, "SKIP": 0}
        pattern_res = {}
        cond_res    = {"bull": {"W": 0,"L": 0},"bear": {"W": 0,"L": 0},"sideways": {"W": 0,"L": 0}}
        total_pnl   = 0.0; window = 60
        for i in range(window, len(klines)-10):
            wk = klines[i-window:i]; price = float(klines[i][4])
            closes = [float(k[4]) for k in wk]
            e20 = calculate_ema(closes, 20); e50 = calculate_ema(closes, 50)
            rng = ((max(closes[-20:])-min(closes[-20:]))/min(closes[-20:]))*100 if min(closes[-20:])>0 else 0
            if e20 and e50:
                if e20 > e50*1.02:   cond = "bull"
                elif e20 < e50*0.98: cond = "bear"
                else:                cond = "sideways" if rng < 5 else ("bull" if price > e50 else "bear")
            else: cond = "sideways"
            bt    = 1 if (e20 and e50 and e20 > e50) else -1
            found = detect_patterns(symbol, wk, price, bt)
            if not found: continue
            best = max(found, key=lambda x: x[1])
            if best[1] < MIN_PRIMARY_SCORE: continue
            atr = calculate_atr(wk)
            if atr == 0: continue
            entry = price; direction = best[2]
            sl = entry - atr*ATR_SL_MULTIPLIER if direction=="BUY" else entry + atr*ATR_SL_MULTIPLIER
            tp = entry + atr*ATR_TP_MULTIPLIER if direction=="BUY" else entry - atr*ATR_TP_MULTIPLIER
            hit = "SKIP"
            for j in range(i+1, min(i+96, len(klines))):
                fh = float(klines[j][2]); fl = float(klines[j][3])
                if direction == "BUY":
                    if fh >= tp: hit = "WIN";  break
                    if fl <= sl: hit = "LOSS"; break
                else:
                    if fl <= tp: hit = "WIN";  break
                    if fh >= sl: hit = "LOSS"; break
            if hit == "SKIP": results["SKIP"] += 1; continue
            results[hit] += 1; cond_res[cond]["W" if hit=="WIN" else "L"] += 1
            pnl = (abs(tp-entry)/entry)*100*5 if hit=="WIN" else -(abs(sl-entry)/entry)*100*5
            total_pnl += pnl; pat = best[0]
            if pat not in pattern_res: pattern_res[pat] = {"W": 0,"L": 0}
            pattern_res[pat]["W" if hit=="WIN" else "L"] += 1
        total = results["WIN"] + results["LOSS"]
        wr    = (results["WIN"] / total * 100) if total > 0 else 0
        r  = f"📊 <b>{BOT_HEADER} Backtest</b>\n"
        r += f"🔍 <b>{symbol}</b>\n{S()}\n\n"
        r += f"📈 Candles: {len(klines)} x 15m\n"
        r += f"🔄 Trades: {total} | Skipped: {results['SKIP']}\n"
        r += f"✅ Wins: {results['WIN']} | ❌ Losses: {results['LOSS']}\n"
        r += f"🎯 Win Rate: <b>{wr:.1f}%</b>\n"
        r += f"💰 Simulated PnL: {fmt_pnl(total_pnl)}\n\n"
        r += f"{S()}\n<b>By Market Condition:</b>\n"
        for cond, res in cond_res.items():
            ct = res["W"]+res["L"]; wr2 = (res["W"]/ct*100) if ct > 0 else 0
            r += f"  {cond.capitalize()}: {res['W']}W / {res['L']}L ({wr2:.1f}%)\n"
        r += f"\n{S()}\n<b>Top Patterns:</b>\n"
        for pat, res in sorted(pattern_res.items(), key=lambda x: x[1]["W"], reverse=True)[:5]:
            ct = res["W"]+res["L"]; wr2 = (res["W"]/ct*100) if ct > 0 else 0
            r += f"  • {pat}: {wr2:.1f}% ({ct} trades)\n"
        r += S()
        return r
    except Exception as e:
        logger.error(f"Backtest {symbol}: {e}", exc_info=True)
        return f"Backtest failed: {e}"

# ================= TEXT HELPERS =================
def get_active_trades_text():
    if not active_trades:
        return f"📊 <b>{BOT_HEADER}</b>\n{S()}\n⚪ No active trades.\n{S()}"
    text  = f"📊 <b>{BOT_HEADER} Active Trades ({len(active_trades)})</b>\n{S()}\n\n"
    for coin, t in active_trades.items():
        sl_pct = abs(t["entry"]-t["sl"])/t["entry"]*100
        tp_pct = abs(t["tp"]-t["entry"])/t["entry"]*100
        text += f"🔹 <b>{coin}</b> — {t['direction']} | {t['leverage']}x\n"
        text += f"   💰 Entry: {format_price(t['entry'])}\n"
        text += f"   🎯 TP: {format_price(t['tp'])} (+{tp_pct:.1f}%)\n"
        text += f"   🛑 SL: {format_price(t['sl'])} (-{sl_pct:.1f}%)\n\n"
    text += S()
    return text

def get_pattern_stats_text():
    text = f"📈 <b>{BOT_HEADER} Pattern Performance</b>\n{S()}\n\n"
    for pat, s in sorted(pattern_stats.items(), key=lambda x: x[1]["signals"], reverse=True)[:10]:
        if s["signals"] > 0:
            wr   = (s["wins"] / s["signals"]) * 100
            flag = "🔴" if wr < 40 else "🟡" if wr < 60 else "🟢"
            susp = " 🔒" if is_pattern_suspended(pat) else ""
            text += f"{flag} <b>{pat}</b>{susp}\n"
            text += f"   Signals: {s['signals']} | Win: {wr:.1f}% | PnL: {fmt_pnl(s['total_pnl'])}\n\n"
    text += S()
    return text

def get_10day_summary_text():
    today = datetime.now(IST).date()
    text  = f"📅 <b>{BOT_HEADER} Last 10 Days</b>\n{S()}\n\n"
    ow = ol = 0; op = 0.0
    for days_ago in range(9, -1, -1):
        day   = today - timedelta(days=days_ago)
        dt    = [j for j in trade_journal if j.get("date") == str(day)]
        w     = sum(1 for t in dt if t["result"] == "WIN")
        l     = sum(1 for t in dt if t["result"] == "LOSS")
        total = w + l; pnl = sum(t["pnl"] for t in dt)
        wr    = (w / total * 100) if total > 0 else 0
        ow   += w; ol += l; op += pnl
        ds    = day.strftime("%d %b")
        if total == 0:
            text += f"⚪ <b>{ds}</b> — No trades\n"
        else:
            em = "✅" if w > l else "❌" if l > w else "➖"
            text += f"{em} <b>{ds}</b>: {w}W/{l}L | WR:{wr:.0f}% | {fmt_pnl(pnl)}\n"
    ot = ow + ol; owr = (ow / ot * 100) if ot > 0 else 0
    text += f"\n{S()}\n<b>10 Day Total</b>\n"
    text += f"✅ {ow}W  ❌ {ol}L\n"
    text += f"🎯 Win Rate: <b>{owr:.1f}%</b>\n"
    text += f"💰 Total PnL: {fmt_pnl(op)}\n"
    text += f"📊 Avg/day: {fmt_pnl(op/10)}\n{S()}"
    return text

def get_streak_text():
    if not trade_journal:
        return f"🔥 <b>{BOT_HEADER} Streak</b>\n{S()}\nNo trades yet.\n{S()}"
    st = trade_journal[-1]["result"]; sc = 0
    for t in reversed(trade_journal):
        if t["result"] == st: sc += 1
        else: break
    em   = "🔥" if st == "WIN" else "❄️"
    text  = f"{em} <b>{BOT_HEADER} Streak</b>\n{S()}\n"
    text += f"{'Winning' if st=='WIN' else 'Losing'} streak: <b>{sc} trades</b>\n"
    if st == "WIN"  and sc >= 3: text += "\n🔥 You are on fire! Stay disciplined."
    elif st == "LOSS" and sc >= 2: text += "\n⚠️ Losing streak. Consider reducing size."
    text += f"\n{S()}"
    return text

def get_best_text():
    if not trade_journal:
        return f"⭐ <b>{BOT_HEADER} Best Performers</b>\n{S()}\nNo data yet.\n{S()}"
    cs = {}; ps2 = {}
    for t in trade_journal:
        c = t["coin"]
        if c not in cs: cs[c] = {"W": 0,"L": 0,"pnl": 0.0}
        cs[c]["W" if t["result"]=="WIN" else "L"] += 1; cs[c]["pnl"] += t["pnl"]
        p = t["pattern"]
        if p not in ps2: ps2[p] = {"W": 0,"L": 0}
        ps2[p]["W" if t["result"]=="WIN" else "L"] += 1
    text = f"⭐ <b>{BOT_HEADER} Best Performers</b>\n{S()}\n\n"
    text += "<b>🏆 Top Coins:</b>\n"
    sc = sorted(cs.items(),
                key=lambda x: (x[1]["W"]/(x[1]["W"]+x[1]["L"])) if (x[1]["W"]+x[1]["L"]) > 0 else 0,
                reverse=True)[:3]
    for i, (c, s) in enumerate(sc, 1):
        tot = s["W"]+s["L"]; wr = (s["W"]/tot*100) if tot > 0 else 0
        text += f"  {i}. <b>{c}</b> — {wr:.1f}% WR | {fmt_pnl(s['pnl'])}\n"
    text += "\n<b>🎯 Top Patterns:</b>\n"
    sp = sorted(ps2.items(),
                key=lambda x: (x[1]["W"]/(x[1]["W"]+x[1]["L"])) if (x[1]["W"]+x[1]["L"]) > 0 else 0,
                reverse=True)[:3]
    for i, (p, s) in enumerate(sp, 1):
        tot = s["W"]+s["L"]; wr = (s["W"]/tot*100) if tot > 0 else 0
        text += f"  {i}. <b>{p}</b> — {wr:.1f}% WR ({tot} trades)\n"
    text += S()
    return text

def get_risk_text():
    if not active_trades:
        return f"🛡️ <b>{BOT_HEADER} Risk</b>\n{S()}\nNo active trades.\n{S()}"
    text = f"🛡️ <b>{BOT_HEADER} Risk Exposure</b>\n{S()}\n\n"
    total_risk = 0.0
    for coin, t in active_trades.items():
        rp = abs(t["entry"]-t["sl"])/t["entry"]*100*t["leverage"]
        total_risk += rp
        text += f"🔸 <b>{coin}</b>: {rp:.1f}% max loss at SL\n"
    text += f"\n{S()}\n📊 Total Risk: {total_risk:.1f}%\n"
    text += f"📌 Active: {len(active_trades)}/{MAX_ACTIVE_TRADES}\n"
    text += f"🛡️ Circuit Breaker: {'🔴 ACTIVE' if check_circuit_breaker() else '🟢 OK'}\n{S()}"
    return text

def get_learning_text():
    if not learning_notes:
        return f"🧠 <b>{BOT_HEADER} Learning</b>\n{S()}\nNo insights yet. Need more trade data.\n{S()}"
    text  = f"🧠 <b>{BOT_HEADER} Bot Learning</b>\n{S()}\n\n"
    text += "<b>📊 Market Memory:</b>\n"
    for cond in ["bull","bear","sideways"]:
        mem = market_memory[cond]; tot = mem["wins"]+mem["losses"]
        wr  = (mem["wins"]/tot*100) if tot > 0 else 0
        text += f"  {cond.capitalize()}: {mem['wins']}W/{mem['losses']}L ({wr:.1f}%) | Best: {mem['best_pattern'] or 'N/A'}\n"
    susp = [p for p,d in consecutive_loss_patterns.items()
            if d.get("consecutive_losses",0) >= CONSEC_LOSS_SUSPEND and d.get("suspended_until")]
    text += f"\n<b>🔒 Suspended:</b> {', '.join(susp) if susp else 'None'}\n"
    text += f"\n{S()}\n<b>🎯 Pattern Weight Evolution:</b>\n"
    text += "<i>(How the bot has adjusted each pattern based on results)</i>\n\n"
    for pat, s in sorted(pattern_stats.items(),
                         key=lambda x: x[1].get("weight", 1.0), reverse=True):
        w    = s.get("weight", 1.0)
        sigs = s.get("signals", 0)
        if sigs > 0:
            trend = "📈 Boosted" if w > 1.0 else "📉 Reduced" if w < 1.0 else "➖ Neutral"
            text += f"  {trend} <b>{pat}</b> ({w:.2f}x) — {sigs} signals\n"
    text += f"\n{S()}\n<b>💡 Latest Insights:</b>\n\n"
    for note in learning_notes[-10:]: text += f"• {note}\n\n"
    text += S()
    return text

def expire_pending_signals():
    now     = get_ist_datetime()
    expired = [c for c,s in list(pending_signals.items())
               if s.get("expires_at") and now > s["expires_at"]]
    for coin in expired:
        del pending_signals[coin]
        send_telegram(f"⏰ <b>{BOT_HEADER}</b>\nSignal expired: <b>{coin}</b> — not activated in time.")

def check_price_alerts():
    triggered = []
    for sym, alert in list(price_alerts.items()):
        price = get_price(sym + "USDT")
        if not price: continue
        if alert["direction"] == "above" and price >= alert["price"]:
            send_telegram(
                f"🔔 <b>{BOT_HEADER} PRICE ALERT</b>\n{S()}\n"
                f"<b>{sym}</b> is now ABOVE {format_price(alert['price'])}\n"
                f"Current: {format_price(price)}\n{S()}")
            triggered.append(sym)
        elif alert["direction"] == "below" and price <= alert["price"]:
            send_telegram(
                f"🔔 <b>{BOT_HEADER} PRICE ALERT</b>\n{S()}\n"
                f"<b>{sym}</b> is now BELOW {format_price(alert['price'])}\n"
                f"Current: {format_price(price)}\n{S()}")
            triggered.append(sym)
    for sym in triggered: del price_alerts[sym]
    if triggered: save_alerts()

# ================= PATTERN DETECTION =================
def detect_patterns(symbol, klines, price, btc_trend):
    if len(klines) < 50: return []
    closes  = [float(k[4]) for k in klines]; opens = [float(k[1]) for k in klines]
    highs   = [float(k[2]) for k in klines]; lows  = [float(k[3]) for k in klines]
    vols    = [float(k[5]) for k in klines]; avg_vol = sum(vols[-20:]) / 20
    rsi     = calculate_rsi(closes)
    ema20   = calculate_ema(closes, 20); ema50 = calculate_ema(closes, 50)
    adx     = calculate_adx(klines)
    if ((max(highs[-20:])-min(lows[-20:]))/price)*100 < 1.8: return []
    if adx < ADX_MIN_TREND: return []
    p   = []
    sup = min(lows[-30:-1]); res = max(highs[-30:-1])
    if ema20 and closes[-1]>highs[-2] and closes[-2]>highs[-3] and price>ema20 and btc_trend==1:
        p.append(("Bull Flag Break",94,"BUY"))
    if ema20 and ema50 and closes[-1]<lows[-2] and closes[-2]<lows[-3] and price<ema20 and price<ema50*0.99 and btc_trend==-1:
        p.append(("Bear Flag Break",94,"SELL"))
    if closes[-1]>max(highs[-20:-1]) and vols[-1]>avg_vol*1.5:
        if btc_trend==1: p.append(("Breakout",88,"BUY"))
    elif closes[-1]<min(lows[-20:-1]) and vols[-1]>avg_vol*1.5:
        if btc_trend==-1: p.append(("Breakout",88,"SELL"))
    if opens[-2]>closes[-2] and opens[-1]<closes[-2] and closes[-1]>opens[-2]:
        if btc_trend==1: p.append(("Bullish Engulfing",90,"BUY"))
    elif opens[-2]<closes[-2] and opens[-1]>closes[-2] and closes[-1]<opens[-2]:
        if btc_trend==-1: p.append(("Bearish Engulfing",90,"SELL"))
    if ema20 and ema50:
        if price>ema20>ema50 and btc_trend==1:    p.append(("EMA Trend",85,"BUY"))
        elif price<ema20<ema50 and btc_trend==-1: p.append(("EMA Trend",85,"SELL"))
    if ema20 and abs(price-ema20)/ema20<0.005:
        p.append(("Pullback to 20 EMA",82,"BUY" if price>ema20 else "SELL"))
    if rsi<30:   p.append(("RSI Reversal",80,"BUY"))
    elif rsi>70: p.append(("RSI Reversal",80,"SELL"))
    mom = (closes[-1]-closes[-3])/closes[-3]*100 if len(closes)>3 else 0
    if mom>3 and btc_trend==1:    p.append(("Momentum Surge",87,"BUY"))
    elif mom<-3 and btc_trend==-1: p.append(("Momentum Surge",87,"SELL"))
    if vols[-1]>avg_vol*3.5:
        p.append(("Volume Spike",84,"BUY" if closes[-1]>opens[-1] else "SELL"))
    if price<=sup*1.005 and closes[-1]>opens[-1]: p.append(("Support Bounce",88,"BUY"))
    if price>=res*0.995 and closes[-1]<opens[-1]: p.append(("Resistance Rejection",88,"SELL"))
    if len(lows)>40:
        if abs(min(lows[-40:-20])-min(lows[-10:]))/price<0.005: p.append(("Double Bottom",90,"BUY"))
        if abs(max(highs[-40:-20])-max(highs[-10:]))/price<0.005: p.append(("Double Top",90,"SELL"))
    if price>res and vols[-1]>avg_vol*2.5 and btc_trend==1: p.append(("Volume Breakout",91,"BUY"))
    return p

# ================= TRAILING SL & PARTIAL TP =================
def update_trailing_sl(coin, trade, price):
    trail = abs(trade["tp"]-trade["entry"])*0.3
    if trade["direction"]=="BUY":
        new_sl = price - trail
        if new_sl > trade["sl"]: active_trades[coin]["sl"]=new_sl; save_active_trades()
    else:
        new_sl = price + trail
        if new_sl < trade["sl"]: active_trades[coin]["sl"]=new_sl; save_active_trades()

def check_partial_tp(coin, trade, price, pnl):
    if trade.get("partial_tp_taken"): return
    halfway = ((abs(trade["tp"]-trade["entry"])/2)/trade["entry"])*100*trade["leverage"]
    if pnl >= halfway:
        active_trades[coin]["partial_tp_taken"] = True
        active_trades[coin]["sl"]               = trade["entry"]
        save_active_trades()
        send_telegram(
            f"💰 <b>{BOT_HEADER} PARTIAL TP HIT</b>\n{S()}\n"
            f"🔹 <b>{coin}</b> — 50% position closed\n"
            f"📍 Closed at: {format_price(price)}\n"
            f"🛑 SL moved to entry (breakeven) 🎯\n"
            f"📈 PnL so far: {fmt_pnl(pnl)}\n{S()}"
        )

# ================= FORMAT AND SEND =================
def get_news_headlines(coin):
    if not NEWS_API_KEY: return []
    try:
        res = requests.get("https://cryptopanic.com/api/v1/posts/",
                           params={"auth_token": NEWS_API_KEY, "currencies": coin, "kind": "news"},
                           timeout=5)
        return [p["title"] for p in res.json().get("results",[])[:2]]
    except Exception as e:
        logger.warning(f"headlines {coin}: {e}"); return []

def format_and_send(setup, coin, is_river=False, is_instant=False, market_condition="bull"):
    global pending_signals, sent_coins, daily_sent_coins
    if check_circuit_breaker(): return False
    if not is_good_trading_session(): return False
    if coin in daily_sent_coins and not is_instant:
        logger.info(f"{coin} already sent today"); return False

    live_price = get_price(setup["symbol"])
    if not live_price: return False
    entry = live_price

    if abs(entry-setup["scan_price"])/setup["scan_price"] > 0.01:
        logger.info(f"{coin} rejected — drifted"); return False

    klines_15m = get_klines(setup["symbol"],"15m",100)
    klines_1h  = get_klines(setup["symbol"],"1h",50)
    if not klines_15m: return False

    closes  = [float(x[4]) for x in klines_15m]
    atr_15m = calculate_atr(klines_15m)
    atr_1h  = calculate_atr(klines_1h) if len(klines_1h) >= 15 else atr_15m
    atr_pct = (atr_1h/entry)*100 if entry > 0 else 0

    vol_ok     = is_volume_confirmed(klines_15m)
    rsi_ok     = is_rsi_valid(closes, setup["direction"])
    funding_ok = is_funding_favorable(setup["symbol"], setup["direction"])

    if not vol_ok:
        logger.info(f"{coin} rejected — volume not confirmed"); return False
    if not rsi_ok:
        logger.info(f"{coin} rejected — RSI invalid"); return False
    if not is_volatility_normal(klines_15m):
        logger.info(f"{coin} rejected — volatility abnormal"); return False
    if not funding_ok:
        logger.info(f"{coin} rejected — funding rate unfavorable"); return False

    candle_strong = is_candle_strong(klines_15m)
    # Candle strength is informational — weak candles get noted but not blocked
    if not candle_strong:
        logger.info(f"{coin} — weak candle body noted")

    st_15m = calculate_supertrend(klines_15m, ST_PERIOD, ST_MULTIPLIER)
    st_1h  = calculate_supertrend(klines_1h,  ST_PERIOD, ST_MULTIPLIER) if klines_1h else st_15m
    st_ok  = (st_15m == setup["direction"]) and (st_1h == setup["direction"])
    # SuperTrend is informational — shown in signal but does not block
    if not st_ok:
        logger.info(f"{coin} — SuperTrend disagrees ({st_15m}/{st_1h}) — noted but not blocking")

    vwap      = calculate_vwap(klines_15m)
    vwap_ok   = False; vwap_label = "➖ N/A"
    if vwap:
        if setup["direction"]=="BUY"  and entry > vwap:
            vwap_ok=True; vwap_label=f"✅ Above {format_price(vwap)}"
        elif setup["direction"]=="SELL" and entry < vwap:
            vwap_ok=True; vwap_label=f"✅ Below {format_price(vwap)}"
        else:
            vwap_label=f"⚠️ {'Below' if setup['direction']=='BUY' else 'Above'} {format_price(vwap)}"

    zones    = detect_supply_demand_zones(klines_15m)
    zone_ok, zone_label = is_in_zone(entry, setup["direction"], zones)

    div       = detect_rsi_divergence(closes)
    div_label = ""
    if div=="BULLISH_DIV" and setup["direction"]=="BUY":   div_label="📊 Bullish RSI Divergence ✅"
    elif div=="BEARISH_DIV" and setup["direction"]=="SELL": div_label="📊 Bearish RSI Divergence ✅"

    oi_rising = get_oi_trend(setup["symbol"])
    oi_label  = "✅ Rising" if oi_rising else "⚠️ Falling" if oi_rising is False else "➖ N/A"
    whale     = has_whale_activity(setup["symbol"])
    adx_val   = calculate_adx(klines_15m)

    tf_score = setup.get("tf_score", get_timeframe_score(setup["symbol"], setup["direction"]))
    # Always recalculate leverage fresh using 1h ATR for accuracy
    lev      = get_smart_leverage(setup["symbol"], atr_pct, setup["setup_score"])
    logger.info(f"{coin} leverage calculated: {lev}x | ATR%: {atr_pct:.2f} | Score: {setup['setup_score']:.1f}")

    sl = get_structure_sl(klines_15m, setup["direction"], entry, atr_1h)
    tp = entry+atr_1h*ATR_TP_MULTIPLIER if setup["direction"]=="BUY" else entry-atr_1h*ATR_TP_MULTIPLIER

    profit_target = (abs(tp-entry)/entry)*100*lev
    logger.info(f"{coin} profit target: {profit_target:.2f}% | Min: {MIN_PROFIT_TARGET}% | TP: {format_price(tp)} | SL: {format_price(sl)}")
    if profit_target < MIN_PROFIT_TARGET:
        risk = abs(tp-entry)/entry
        if risk > 0:
            needed = int(MIN_PROFIT_TARGET/(risk*100))+1
            logger.info(f"{coin} adjusting leverage: {lev}x -> {needed}x to meet profit target")
            if needed <= 20: lev=needed; profit_target=(abs(tp-entry)/entry)*100*lev
            else:
                logger.info(f"{coin} rejected — can't meet profit target even at 20x")
                return False

    setup["leverage"] = lev
    price_range    = (max(closes[-10:])-min(closes[-10:]))/10
    eta            = int(abs(tp-entry)/(price_range if price_range>0 else 0.001)*15)
    expiry_minutes = INSTANT_EXPIRY_MINUTES if is_instant else SIGNAL_EXPIRY_MINUTES
    expiry_time    = get_ist_datetime()+timedelta(minutes=expiry_minutes)
    expiry_str     = expiry_time.strftime("%I:%M %p IST")
    mom     = (closes[-1]-closes[-3])/closes[-3]*100
    rsi_val = calculate_rsi(closes)
    news    = get_news_headlines(coin)

    tf_map = {
        3: "4h ✅  1h ✅  💪 STRONG",
        2: "4h ✅  1h ⚠️  ✅ GOOD",
        1: "4h ⚠️  1h ✅  ⚠️ MODERATE",
        0: "4h ⚠️  1h ⚠️  🔴 COUNTER-TREND"
    }
    tf_label  = tf_map.get(tf_score, "N/A")
    cond_em   = {"bull":"📈","bear":"📉","sideways":"➡️"}.get(market_condition,"")
    cond_lbl  = f"{cond_em} {market_condition.capitalize()} Market"
    grade     = get_signal_grade(setup["setup_score"], whale, oi_rising, tf_score,
                                 vol_ok, rsi_ok, funding_ok, st_ok, vwap_ok, zone_ok, adx_val)
    sl_pct    = abs(entry-sl)/entry*100
    tp_pct    = abs(tp-entry)/entry*100
    rr_ratio  = tp_pct / sl_pct if sl_pct > 0 else 0
    # Position size suggestion based on 1% account risk
    suggested_size_pct = 1.0 / (sl_pct * lev / 100) * 100 if sl_pct > 0 else 2.0
    suggested_size_pct = min(suggested_size_pct, 10.0)  # cap at 10% of capital

    if is_instant: header = f"⚡ <b>{BOT_HEADER}</b>\n<b>INSTANT SIGNAL — {coin}</b>"
    elif is_river: header = f"🌊 <b>{BOT_HEADER}</b>\n<b>RIVER SIGNAL</b>"
    else:          header = f"🔥 <b>{BOT_HEADER}</b>\n<b>VERIFIED SETUP — {coin}</b>"

    msg  = f"{header}\n"
    msg += f"{S('═')}\n"
    msg += f"🏆 Score: <b>{int(setup['setup_score'])}/100</b>  |  {grade}\n"
    msg += f"{S()}\n\n"
    msg += f"📢 <b>{setup['direction']}</b>  |  Leverage: <b>{lev}x</b>  |  {cond_lbl}\n\n"
    msg += f"💰 Entry:  <code>{format_price(entry)}</code>\n"
    msg += f"🎯 TP:     <code>{format_price(tp)}</code>  (+{tp_pct:.2f}%)\n"
    msg += f"🛑 SL:     <code>{format_price(sl)}</code>  (-{sl_pct:.2f}%)\n\n"
    msg += f"📈 Profit Target: <b>{profit_target:.2f}%</b>  |  RR: <b>1:{rr_ratio:.1f}</b>\n"
    msg += f"💡 Suggested Size: ~{suggested_size_pct:.1f}% of capital\n"
    msg += f"{S()}\n"
    # Show adjusted score vs base for transparency
    primary_name  = setup["pattern"].split(" + ")[0]
    base_s        = next((x[1] for x in detect_patterns(setup["symbol"], klines_15m, entry, 1)
                          if x[0] == primary_name), setup["setup_score"])
    adj_s         = get_adjusted_score(primary_name, base_s, market_condition)
    score_note    = f" (base:{base_s:.0f} → live:{adj_s:.0f})" if abs(adj_s - base_s) > 1 else ""
    msg += f"📌 Pattern:     {setup['pattern']}{score_note}\n"
    msg += f"📊 RSI: {rsi_val:.1f}  |  ADX: {adx_val:.1f}  |  Momentum: {mom:+.2f}%\n"
    msg += f"📊 TF Align:    {tf_label}\n"
    msg += f"📊 SuperTrend:  {'✅ Confirmed' if st_ok else '⚠️ Mixed'} ({st_15m}/{st_1h})\n"
    msg += f"📊 VWAP:        {vwap_label}\n"
    msg += f"📊 OI:          {oi_label}  |  🐋 Whale: {'✅ Yes' if whale else '❌ No'}\n"
    if zone_ok:   msg += f"📍 Zone:        ✅ In {'demand' if setup['direction']=='BUY' else 'supply'} zone {zone_label}\n"
    if div_label: msg += f"{div_label}\n"
    msg += f"{S()}\n"
    msg += f"⏳ ETA: ~{eta} mins  |  ⏰ Expires: {expiry_str}\n"
    msg += f"✏️ ATR (1h): {format_price(atr_1h)}"
    if is_instant: msg += f"\n\n⚡ <b>INSTANT — Act within {expiry_minutes} mins!</b>"
    if news:
        msg += f"\n{S()}\n<b>📰 Related News:</b>\n"
        for n in news: msg += f"• {n[:70]}\n"
    msg += f"\n{S('═')}\n🕐 {get_ist_time()}"

    setup.update({
        "entry": entry,"sl": sl,"tp": tp,
        "timestamp": get_ist_datetime(),"expires_at": expiry_time,
        "reversal_alerted": False,"breakeven_sent": False,
        "partial_tp_taken": False,"tf_score": tf_score,
        "market_condition": market_condition
    })
    pending_signals[coin] = setup
    reply_markup = {"inline_keyboard": [[
        {"text": "✅ Activate Trade","callback_data": f"ACTIVATE_{coin}"},
        {"text": "❌ Ignore",         "callback_data": f"IGNORE_{coin}"}
    ]]}
    if send_telegram(msg, reply_markup=reply_markup):
        sent_coins.append(coin); daily_sent_coins.add(coin); save_daily_sent()
        logger.info(f"Signal: {coin}|{setup['direction']}|Score:{setup['setup_score']}|{grade}")
        return True
    return False

# ================= BATCH =================
def send_hourly_batch():
    global hourly_queue, last_batch_time, sent_coins
    if not hourly_queue: return
    sq = sorted(hourly_queue.values(), key=lambda x: x["setup_score"], reverse=True)
    sc = 0
    for s in sq:
        if s["coin"]=="RIVER": continue
        if sc >= MAX_SIGNALS_PER_BATCH: break
        if format_and_send(s, s["coin"], market_condition=s.get("market_condition","bull")): sc += 1
    for s in sq:
        if s["coin"] in hourly_queue: del hourly_queue[s["coin"]]
    sent_coins=[]; last_batch_time=time.time()

# ================= ACTIVE TRADE MONITORING =================
def check_active_trades():
    for coin, trade in list(active_trades.items()):
        price = get_price(trade["symbol"])
        if not price: continue
        if trade["direction"]=="BUY":
            pnl = ((price-trade["entry"])/trade["entry"])*100*trade["leverage"]
        else:
            pnl = ((trade["entry"]-price)/trade["entry"])*100*trade["leverage"]
        update_trailing_sl(coin, trade, price)
        check_partial_tp(coin, trade, price, pnl)
        if not trade.get("reversal_alerted", False):
            klines = get_klines(trade["symbol"],"15m",20)
            if klines:
                closes = [float(x[4]) for x in klines]
                ema20  = calculate_ema(closes, 20)
                if ema20:
                    rev = ((trade["direction"]=="BUY"  and price<ema20*0.995) or
                           (trade["direction"]=="SELL" and price>ema20*1.005))
                    if rev:
                        send_telegram(
                            f"⚠️ <b>{BOT_HEADER} TREND REVERSAL</b>\n{S()}\n"
                            f"🔹 <b>{coin}</b> — Price broke EMA20\n"
                            f"Consider reviewing your position.\n{S()}"
                        )
                        active_trades[coin]["reversal_alerted"]=True; save_active_trades()
        if not trade.get("breakeven_sent",False) and pnl >= 10:
            send_telegram(
                f"🟡 <b>{BOT_HEADER} BREAK-EVEN ALERT</b>\n{S()}\n"
                f"🔹 <b>{coin}</b> reached +10% profit\n"
                f"💡 Consider moving SL to entry.\n"
                f"📈 Current PnL: {fmt_pnl(pnl)}\n{S()}"
            )
            active_trades[coin]["breakeven_sent"]=True; save_active_trades()
        hit = None
        if trade["direction"]=="BUY":
            if price>=trade["tp"]:   hit="WIN"
            elif price<=trade["sl"]: hit="LOSS"
        else:
            if price<=trade["tp"]:   hit="WIN"
            elif price>=trade["sl"]: hit="LOSS"
        if hit:
            with trade_lock:
                primary = trade["pattern"].split(" + ")[0]
                if primary in pattern_stats:
                    pattern_stats[primary]["signals"]  +=1
                    pattern_stats[primary]["total_pnl"]+=pnl
                    pattern_stats[primary]["wins" if hit=="WIN" else "losses"]+=1
                increment_daily_losses(pnl)
                # 4-hour cooldown on this coin after a loss
                if hit == "LOSS":
                    coin_cooldowns[coin] = get_ist_datetime() + timedelta(hours=4)
                    logger.info(f"Cooldown set for {coin} — 4 hours")
                duration = ""
                if trade.get("timestamp"):
                    mins     = int((get_ist_datetime()-trade["timestamp"]).total_seconds()/60)
                    duration = f"{mins} mins"
                mc = trade.get("market_condition","bull")
                trade_journal.append({
                    "date": str(datetime.now(IST).date()),"coin": coin,
                    "direction": trade["direction"],"pattern": primary,
                    "entry": trade["entry"],"exit": price,"pnl": pnl,
                    "result": hit,"duration": duration,
                    "tf_score": trade.get("tf_score",0),"market_condition": mc
                })
                save_journal()
                learn_from_trade(coin,primary,hit,pnl,mc,trade.get("tf_score",0))
            em = "✅" if hit=="WIN" else "🛑"
            send_telegram(
                f"{em} <b>{BOT_HEADER} TRADE CLOSED — {coin}</b>\n{S('═')}\n"
                f"🔹 Result: <b>{hit}</b>\n"
                f"💰 Entry:   {format_price(trade['entry'])}\n"
                f"📍 Exit:    {format_price(price)}\n"
                f"📌 Pattern: {primary}\n"
                f"⏱️ Duration: {duration}\n"
                f"📈 PnL: {fmt_pnl(pnl)}\n{S('═')}"
            )
            del active_trades[coin]; save_active_trades(); save_trade_history()
            logger.info(f"Closed: {coin}|{hit}|{pnl:.2f}%")

# ================= TELEGRAM POLLING =================
def poll_telegram():
    global last_update_id
    while True:
        try:
            params = {}
            if last_update_id is not None: params["offset"] = last_update_id+1
            res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                               params=params, timeout=15)
            if res.status_code != 200: time.sleep(2); continue
            for update in res.json().get("result",[]):
                last_update_id = update["update_id"]
                if "callback_query" in update:
                    try:
                        cb   = update["callback_query"]
                        data = cb.get("data", "")
                        cbid = cb.get("id", "")

                        # Answer callback immediately so button stops loading
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cbid, "text": "Processing..."},
                                timeout=10
                            )
                        except Exception as e:
                            logger.warning(f"answerCallback failed: {e}")

                        # Parse action and coin name safely
                        if "_" not in data:
                            logger.warning(f"Invalid callback data: {data}")
                            continue
                        action = data.split("_", 1)[0]   # ACTIVATE or IGNORE
                        coin   = data.split("_", 1)[1]   # coin name

                        logger.info(f"Callback received: action={action} coin={coin} pending={list(pending_signals.keys())}")

                        if action == "ACTIVATE":
                            if coin not in pending_signals:
                                # Signal expired — inform user
                                send_telegram(
                                    f"⚠️ <b>{BOT_HEADER}</b>\n"
                                    f"Signal for <b>{coin}</b> has expired.\n"
                                    f"It was not in pending signals.\n"
                                    f"Wait for next signal."
                                )
                                logger.warning(f"ACTIVATE failed — {coin} not in pending_signals")
                            elif coin in active_trades:
                                send_telegram(
                                    f"⚠️ <b>{BOT_HEADER}</b>\n"
                                    f"<b>{coin}</b> is already an active trade."
                                )
                            elif len(active_trades) >= MAX_ACTIVE_TRADES:
                                send_telegram(
                                    f"⚠️ <b>{BOT_HEADER}</b>\n"
                                    f"Max active trades ({MAX_ACTIVE_TRADES}) reached.\n"
                                    f"Close a trade before activating {coin}."
                                )
                            else:
                                try:
                                    # Build clean trade dict
                                    sig = {}
                                    for k, v in pending_signals[coin].items():
                                        sig[k] = v

                                    # Get fresh live price
                                    live_price = get_price(sig.get("symbol", coin + "USDT"))
                                    if live_price and live_price > 0:
                                        sig["entry"] = live_price
                                    elif not sig.get("entry"):
                                        sig["entry"] = sig.get("scan_price", 0)

                                    # Reset trade tracking flags
                                    sig["breakeven_sent"]   = False
                                    sig["partial_tp_taken"] = False
                                    sig["reversal_alerted"] = False
                                    sig["timestamp"]        = get_ist_datetime()
                                    sig["expires_at"]       = None

                                    # Add to active trades
                                    with trade_lock:
                                        active_trades[coin] = sig

                                    # Remove from pending
                                    if coin in pending_signals:
                                        del pending_signals[coin]

                                    # Save to disk
                                    save_active_trades()

                                    # Build confirmation message
                                    entry_p = sig.get("entry", 0)
                                    sl_p    = sig.get("sl", 0)
                                    tp_p    = sig.get("tp", 0)
                                    lev     = sig.get("leverage", 5)
                                    dirn    = sig.get("direction", "?")
                                    pat     = sig.get("pattern", "?")
                                    sl_pct  = abs(entry_p - sl_p) / entry_p * 100 if entry_p > 0 else 0
                                    tp_pct  = abs(tp_p - entry_p) / entry_p * 100 if entry_p > 0 else 0
                                    rr      = tp_pct / sl_pct if sl_pct > 0 else 0

                                    send_telegram(
                                        f"🚀 <b>{BOT_HEADER}</b>\n"
                                        f"{S('═')}\n"
                                        f"✅ <b>{coin}</b> TRADE ACTIVATED\n\n"
                                        f"📢 {dirn} | Leverage: <b>{lev}x</b>\n"
                                        f"💰 Entry: <code>{format_price(entry_p)}</code>\n"
                                        f"🎯 TP:    <code>{format_price(tp_p)}</code>  (+{tp_pct:.2f}%)\n"
                                        f"🛑 SL:    <code>{format_price(sl_p)}</code>  (-{sl_pct:.2f}%)\n"
                                        f"📊 RR:   1:{rr:.1f}\n\n"
                                        f"📌 Pattern: {pat}\n"
                                        f"🕐 Time: {get_ist_time()}\n"
                                        f"{S('═')}\n"
                                        f"✏️ Set your trade on CoinDCX now!"
                                    )
                                    logger.info(f"ACTIVATED: {coin}|{dirn}|Entry:{entry_p}|Lev:{lev}x|RR:1:{rr:.1f}")

                                except Exception as e:
                                    logger.error(f"Activate error {coin}: {e}", exc_info=True)
                                    send_telegram(
                                        f"❌ <b>{BOT_HEADER}</b>\n"
                                        f"Error activating {coin}.\n"
                                        f"Details: {str(e)[:100]}"
                                    )

                        elif action == "IGNORE":
                            if coin in pending_signals:
                                del pending_signals[coin]
                            send_telegram(
                                f"❌ <b>{BOT_HEADER}</b>\n"
                                f"Signal ignored: <b>{coin}</b>"
                            )
                            logger.info(f"IGNORED: {coin}")

                    except Exception as e:
                        logger.error(f"Callback handler error: {e}", exc_info=True)
                elif "message" in update:
                    txt = update["message"].get("text","").strip().lower()
                    if   txt=="/stats":   send_telegram(get_pattern_stats_text())
                    elif txt=="/trades":  send_telegram(get_active_trades_text())
                    elif txt=="/summary": send_telegram(get_10day_summary_text())
                    elif txt=="/streak":  send_telegram(get_streak_text())
                    elif txt=="/best":    send_telegram(get_best_text())
                    elif txt=="/risk":    send_telegram(get_risk_text())
                    elif txt=="/learn":   send_telegram(get_learning_text())
                    elif txt=="/news":
                        send_telegram(f"🔄 <b>{BOT_HEADER}</b>\nFetching latest market news...")
                        send_telegram(get_crypto_news())
                    elif txt=="/pending":
                        if pending_signals:
                            msg=f"⏳ <b>{BOT_HEADER} Pending Signals</b>\n{S()}\n\n"
                            for c,s in pending_signals.items():
                                exp=s.get("expires_at")
                                msg+=(f"🔹 <b>{c}</b> {s['direction']} | Score: {s['setup_score']}\n"
                                      f"   ⏰ Expires: {exp.strftime('%I:%M %p IST') if exp else 'N/A'}\n\n")
                            msg+=S(); send_telegram(msg)
                        else:
                            send_telegram(f"<b>{BOT_HEADER}</b>\nNo pending signals right now.")
                    elif txt=="/alerts":
                        if price_alerts:
                            msg=f"🔔 <b>{BOT_HEADER} Price Alerts</b>\n{S()}\n\n"
                            for sym,a in price_alerts.items():
                                msg+=f"🔸 <b>{sym}</b>: {a['direction']} {format_price(a['price'])}\n"
                            msg+=S(); send_telegram(msg)
                        else:
                            send_telegram(f"<b>{BOT_HEADER}</b>\nNo active price alerts.")
                    elif txt.startswith("/alert"):
                        parts = txt.split()
                        if len(parts) >= 4:
                            try:
                                sym=parts[1].upper(); target=float(parts[2]); direction=parts[3].lower()
                                price_alerts[sym]={"price":target,"direction":direction}
                                save_alerts()
                                send_telegram(f"🔔 <b>{BOT_HEADER}</b>\nAlert set: <b>{sym}</b> {direction} {format_price(target)}")
                            except Exception:
                                send_telegram("Usage: /alert BTC 95000 above")
                        else:
                            send_telegram("Usage: /alert BTC 95000 above")
                    elif txt.startswith("/backtest"):
                        parts = txt.split()
                        bc    = (parts[1].upper() if len(parts)>1 else "BTC")+"USDT"
                        send_telegram(f"🔄 <b>{BOT_HEADER}</b>\nRunning backtest for {bc}...")
                        send_telegram(run_backtest(bc))
                    elif txt=="/cb":
                        if check_circuit_breaker():
                            cu = circuit_breaker_until or "unknown"
                            send_telegram(f"🔴 <b>{BOT_HEADER}</b>\nCircuit Breaker ACTIVE\nResumes at midnight IST.\nLosses today: {daily_losses}/{MAX_DAILY_LOSSES}")
                        else:
                            send_telegram(f"🟢 <b>{BOT_HEADER}</b>\nCircuit Breaker OK\nLosses today: {daily_losses}/{MAX_DAILY_LOSSES}")
                    elif txt=="/patterns":
                        msg  = f"📊 <b>{BOT_HEADER} All 15 Patterns Ranked</b>\n{S()}\n\n"
                        msg += "<b>Ranked by live performance (adjusts over time):</b>\n\n"
                        all_pats = []
                        for pat, s in pattern_stats.items():
                            sigs = s.get("signals", 0)
                            wr   = (s["wins"] / sigs * 100) if sigs > 0 else 0
                            w    = s.get("weight", 1.0)
                            adj  = get_adjusted_score(pat, 80, "bull")
                            all_pats.append((pat, sigs, wr, w, adj))
                        all_pats.sort(key=lambda x: x[4], reverse=True)
                        for i, (pat, sigs, wr, w, adj) in enumerate(all_pats, 1):
                            flag  = "🔴" if wr < 40 and sigs >= 5 else "🟡" if wr < 60 and sigs >= 5 else "🟢"
                            susp  = " 🔒" if is_pattern_suspended(pat) else ""
                            trend = "📈" if w > 1.0 else "📉" if w < 1.0 else "➖"
                            msg  += f"{i:2}. {flag} <b>{pat}</b>{susp}\n"
                            msg  += f"    Signals: {sigs} | WR: {wr:.1f}% | Weight: {trend}{w:.2f}\n\n"
                        msg += S()
                        send_telegram(msg)
                    elif txt=="/help":
                        msg  = f"📖 <b>{BOT_HEADER} Commands</b>\n{S()}\n\n"
                        msg += "📊 <b>Performance</b>\n"
                        msg += "  /trades    — Active trades\n"
                        msg += "  /pending   — Pending signals\n"
                        msg += "  /stats     — Pattern performance\n"
                        msg += "  /patterns  — All 15 patterns ranked live\n"
                        msg += "  /summary   — Last 10 days\n"
                        msg += "  /streak    — Win/loss streak\n"
                        msg += "  /best      — Top coins & patterns\n"
                        msg += "  /risk      — Risk exposure\n"
                        msg += "  /cb        — Circuit breaker status\n\n"
                        msg += "🧠 <b>Intelligence</b>\n"
                        msg += "  /news          — Latest crypto news\n"
                        msg += "  /learn         — Bot learning insights\n"
                        msg += "  /backtest BTC  — Backtest any coin\n\n"
                        msg += "🔔 <b>Alerts</b>\n"
                        msg += "  /alerts               — View alerts\n"
                        msg += "  /alert BTC 95000 above\n\n"
                        msg += S()
                        send_telegram(msg)
        except requests.RequestException as e: logger.error(f"poll request: {e}")
        except Exception as e:                 logger.error(f"poll error: {e}", exc_info=True)
        time.sleep(2)

# ================= REPORTS =================
def send_hourly_report():
    r  = f"📊 <b>{BOT_HEADER} Hourly Report</b>\n"
    r += f"🕐 {get_ist_time()}\n{S()}\n\n"
    r += f"📌 Active: {len(active_trades)}  |  ⏳ Pending: {len(pending_signals)}\n"
    r += f"🛡️ Circuit Breaker: {'🔴 ACTIVE' if check_circuit_breaker() else '🟢 OK'}\n\n"
    r += get_pattern_stats_text()
    send_telegram(r)

def send_live_pnl_update():
    if not active_trades: return
    total_pnl=0.0; wins=losses=0
    msg  = f"⏰ <b>{BOT_HEADER} Live PnL Update</b>\n"
    msg += f"🕐 {get_ist_time()}\n{S()}\n\n"
    for coin, t in active_trades.items():
        price = get_price(t["symbol"])
        if not price: continue
        pnl = (((price-t["entry"])/t["entry"])*100*t["leverage"]
               if t["direction"]=="BUY"
               else ((t["entry"]-price)/t["entry"])*100*t["leverage"])
        total_pnl+=pnl
        if pnl>=3: wins+=1
        elif pnl<=-3: losses+=1
        partial=" 💰 Partial" if t.get("partial_tp_taken") else ""
        msg+=f"🔹 <b>{coin}</b> {t['direction']} | {fmt_pnl(pnl)}{partial}\n"
    total=wins+losses; wr=(wins/total*100) if total>0 else 0
    msg+=f"\n{S()}\n📊 Total PnL: {fmt_pnl(total_pnl)}\n"
    msg+=f"✅ Winning: {wins}  |  ❌ Losing: {losses}\n"
    msg+=f"🎯 Win Rate: {wr:.1f}%\n{S()}"
    send_telegram(msg)

def send_weekly_report():
    today=datetime.now(IST).date()
    week=[today-timedelta(days=i) for i in range(6,-1,-1)]
    wins=losses=0; total_pnl=0.0
    msg  = f"📅 <b>{BOT_HEADER} Weekly Report</b>\n"
    msg += f"📆 {today.strftime('%d %b %Y')}\n{S()}\n\n"
    for day in week:
        dt  = [j for j in trade_journal if j.get("date")==str(day)]
        w   = sum(1 for t in dt if t["result"]=="WIN")
        l   = sum(1 for t in dt if t["result"]=="LOSS")
        pnl = sum(t["pnl"] for t in dt)
        wins+=w; losses+=l; total_pnl+=pnl
        em  = "✅" if w>l else "❌" if l>w else "⚪"
        msg+=f"{em} {day.strftime('%a %d')}: {w}W/{l}L | {fmt_pnl(pnl)}\n"
    total=wins+losses; wr=(wins/total*100) if total>0 else 0
    msg+=f"\n{S()}\n<b>Week Total:</b>\n"
    msg+=f"✅ {wins}W  ❌ {losses}L  |  🎯 {wr:.1f}% WR\n"
    msg+=f"💰 Total PnL: {fmt_pnl(total_pnl)}\n\n"
    msg+=generate_weekly_insight()
    send_telegram(msg)

# ================= RIVER SCAN =================
def scan_river(now, market_condition):
    global last_river_time
    try:
        if "RIVER" not in active_trades and "RIVER" not in pending_signals:
            price  = get_price("RIVERUSDT")
            klines = get_klines("RIVERUSDT","15m",100)
            if not price or not klines or len(klines)<50: return
            found = (detect_patterns("RIVERUSDT",klines,price,1)+
                     detect_patterns("RIVERUSDT",klines,price,-1))
            seen=set(); unique=[]
            for pat in found:
                if (pat[0],pat[2]) not in seen:
                    seen.add((pat[0],pat[2])); unique.append(pat)
            if unique:
                best=max(unique,key=lambda x:x[1])
                if best[1]<MIN_PRIMARY_SCORE: return
                confirmed=list(dict.fromkeys([x[0] for x in unique]))
                primary=best[0]; extras=[p for p in confirmed if p!=primary]
                pt=primary+(" + "+" + ".join(extras[:2]) if extras else "")
                score=min(best[1]+min(len(unique)*0.5,2),99)
                if score>=82:
                    atr=calculate_atr(klines); atr_pct=(atr/price)*100 if price>0 else 0
                    setup={"coin":"RIVER","symbol":"RIVERUSDT","direction":best[2],
                           "pattern":pt,"setup_score":score,
                           "leverage":get_smart_leverage("RIVERUSDT",atr_pct,score),"scan_price":price}
                    format_and_send(setup,"RIVER",is_river=True,
                                    is_instant=score>=INSTANT_SIGNAL_THRESHOLD,
                                    market_condition=market_condition)
        last_river_time=now
    except Exception as e: logger.error(f"River: {e}",exc_info=True)

# ================= COIN SCAN =================
def is_btc_crashing() -> bool:
    """Block all BUY signals if BTC dropped more than 5% in last 4 hours."""
    try:
        klines = get_klines("BTCUSDT", "1h", 5)
        if not klines or len(klines) < 4: return False
        price_now  = float(klines[-1][4])
        price_4h   = float(klines[-4][4])
        drop_pct   = ((price_now - price_4h) / price_4h) * 100
        if drop_pct < -5.0:
            logger.info(f"BTC crashed {drop_pct:.1f}% in 4h — blocking BUY signals")
            return True
        return False
    except Exception: return False

def scan_coins(btc_trend, fng, market_condition):
    btc_crashing = is_btc_crashing()
    for coin in COINS:
        try:
            symbol=coin+"USDT"; price=get_price(symbol); klines=get_klines(symbol,"15m")
            if not price or not klines: continue
            # Skip coin if it is in cooldown after a recent loss
            if coin in coin_cooldowns:
                cooldown_until = coin_cooldowns[coin]
                if get_ist_datetime() < cooldown_until:
                    logger.info(f"Skip {coin} — in cooldown until {cooldown_until.strftime('%H:%M')}")
                    continue
                else:
                    del coin_cooldowns[coin]

            found=detect_patterns(symbol,klines,price,btc_trend)
            if not found: continue

            # All 15 patterns get performance-adjusted scores — none discarded
            scored = get_all_pattern_scores(found, market_condition)

            # Try both directions — pick best qualifying for each
            signal_sent = False
            for direction in ["BUY","SELL"]:
                if signal_sent: break
                dir_pats = [p for p in scored if p[2]==direction]
                if not dir_pats: continue

                best_pat   = dir_pats[0]
                primary    = best_pat[0]
                adj_score  = best_pat[1]
                base_s     = best_pat[3]

                if base_s < MIN_PRIMARY_SCORE:                         continue
                if is_pattern_blacklisted(primary):                     continue
                if is_pattern_suspended(primary):                       continue
                if not is_sentiment_valid(direction, fng):              continue
                if btc_crashing and direction=="BUY":
                    logger.info(f"Skip {coin} BUY — BTC crashing");   continue
                if coin in BTC_CORRELATED and too_many_correlated_active(): continue

                tf_score = get_timeframe_score(symbol, direction)
                if tf_score == -1:
                    logger.info(f"Skip {coin} {direction} — counter-trend"); continue

                # All same-direction patterns shown in label
                extras = [p[0] for p in dir_pats[1:3]]
                pt     = primary + (" + "+" + ".join(extras) if extras else "")

                # Confirmation bonus for multiple patterns agreeing
                confirm_bonus = min(len(dir_pats) * 0.5, 3.0)
                score         = min(adj_score + confirm_bonus, 99)

                if score < MIN_SETUP_SCORE: continue

                atr    = calculate_atr(klines); atr_pct=(atr/price)*100 if price>0 else 0
                lev    = get_smart_leverage(symbol, atr_pct, score)
                setup  = {"coin":coin,"symbol":symbol,"direction":direction,
                          "pattern":pt,"setup_score":score,"leverage":lev,
                          "scan_price":price,"market_condition":market_condition,
                          "tf_score":tf_score}

                if (coin not in active_trades and coin not in pending_signals
                        and len(active_trades) < MAX_ACTIVE_TRADES):
                    is_inst = score >= INSTANT_SIGNAL_THRESHOLD
                    logger.info(f"{'INSTANT' if is_inst else 'SIGNAL'}: {coin}|{direction}|Score:{score:.1f}|{primary}")
                    if format_and_send(setup,coin,is_instant=is_inst,market_condition=market_condition):
                        signal_sent = True

        except Exception as e: logger.error(f"Scan {coin}: {e}",exc_info=True)
        time.sleep(DELAY_BETWEEN_COINS)

# ================= MAIN LOOP =================
def main():
    global last_batch_time,last_river_time,last_hourly_time,last_pnl_update_time,last_weekly_report_day
    load_active_trades(); load_trade_history(); load_journal()
    load_learning(); load_daily_sent(); load_alerts(); load_circuit_breaker()
    threading.Thread(target=poll_telegram, daemon=True).start()
    logger.info(STARTUP_MSG)
    send_telegram(
        f"{STARTUP_MSG}\n\n"
        f"<b>All Systems Active</b>\n{S()}\n\n"
        f"✅ Scanner: {len(COINS)} coins\n"
        f"✅ 4h + 1h Trend Filter\n"
        f"✅ SuperTrend (15m + 1h)\n"
        f"✅ ADX Trend Strength (min {ADX_MIN_TREND})\n"
        f"✅ VWAP Institutional Filter\n"
        f"✅ Supply and Demand Zones\n"
        f"✅ RSI Divergence Detection\n"
        f"✅ Candle Body Strength\n"
        f"✅ Funding Rate + Open Interest\n"
        f"✅ Whale Detection\n"
        f"✅ Fear and Greed Index\n"
        f"✅ 1h ATR Smart SL (min 2%)\n"
        f"✅ Circuit Breaker (losses under -5% only)\n"
        f"✅ CB Auto-Reset at Midnight IST\n"
        f"✅ Instant Signals Score >= {INSTANT_SIGNAL_THRESHOLD}\n"
        f"✅ Smart Position Sizing (safe caps)\n"
        f"✅ Signal Quality Grading A+/A/B/C\n"
        f"✅ Trailing SL + Partial TP\n"
        f"✅ Pattern Learning System\n"
        f"✅ Consecutive Loss Suspension\n"
        f"✅ Duplicate Signal Prevention\n"
        f"✅ Dead Session Filter (2AM-7AM IST)\n"
        f"✅ Price Alert System\n"
        f"✅ Backtest Engine\n"
        f"✅ Weekly AI Insight Report\n"
        f"✅ Min Setup Score: {MIN_SETUP_SCORE}\n\n"
        f"{S()}\n"
        f"📌 Type /help for all commands"
    )
    logger.info(f"{BOT_NAME} {BOT_VERSION} started | {len(COINS)} coins | Score: {MIN_SETUP_SCORE}+")
    while True:
        try:
            btc_price  = get_price("BTCUSDT")
            btc_klines = get_klines("BTCUSDT","1h",100)
            btc_ema50  = calculate_ema([float(x[4]) for x in btc_klines],50)
            if not btc_price or btc_ema50 is None:
                logger.warning("BTC data unavailable"); time.sleep(60); continue
            btc_trend        = 1 if btc_price>btc_ema50 else -1
            fng              = get_fear_greed_index()
            market_condition = detect_market_condition(btc_price,btc_klines)
            logger.info(
                f"BTC:{'BULL' if btc_trend==1 else 'BEAR'} | "
                f"Market:{market_condition} | F&G:{fng} | "
                f"Losses:{daily_losses}/{MAX_DAILY_LOSSES} | "
                f"CB:{'ACTIVE' if check_circuit_breaker() else 'OK'}"
            )
            scan_coins(btc_trend,fng,market_condition)
            check_active_trades()
            expire_pending_signals()
            check_price_alerts()
            now=time.time()
            if (now-last_hourly_time)     >=3600:          send_hourly_report();   last_hourly_time=now
            if (now-last_pnl_update_time) >=3600:          send_live_pnl_update(); last_pnl_update_time=now
            if (now-last_batch_time)      >=BATCH_INTERVAL: send_hourly_batch()
            if (now-last_river_time)      >=RIVER_INTERVAL: scan_river(now,market_condition)
            today=datetime.now(IST).date()
            if today.weekday()==6 and last_weekly_report_day!=today:
                send_weekly_report(); last_weekly_report_day=today
            time.sleep(SCAN_INTERVAL)
        except Exception as e:
            logger.error(f"Main loop: {e}",exc_info=True); time.sleep(60)

if __name__ == "__main__":
    main()
