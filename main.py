import requests
import time
import json
import os
import threading
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import Counter

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("trading_signal_v32.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= CONFIGURATION =================
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

# ================= COINS LIST (90 COINS) =================
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
active_trades    = {}
pending_signals  = {}
hourly_queue     = {}
sent_coins       = []
daily_losses     = 0
last_reset_day   = datetime.now(IST).date()
trade_journal    = []
learning_notes   = []
daily_sent_coins = set()
consecutive_loss_patterns = {}  # tracks consecutive losses per pattern

market_memory = {
    "bull":     {"wins":0,"losses":0,"best_pattern":None},
    "bear":     {"wins":0,"losses":0,"best_pattern":None},
    "sideways": {"wins":0,"losses":0,"best_pattern":None}
}

pattern_stats = {p: {"signals":0,"wins":0,"losses":0,"total_pnl":0} for p in [
    "EMA Trend","Breakout","Pullback to 20 EMA","RSI Reversal","Momentum Surge",
    "Volume Spike","Double Bottom","Double Top","Support Bounce","Resistance Rejection",
    "Bullish Engulfing","Bearish Engulfing","Volume Breakout","Bull Flag Break","Bear Flag Break"
]}

last_update_id          = None
last_batch_time         = 0
last_river_time         = 0
last_hourly_time        = time.time()
last_pnl_update_time    = time.time() + 1800
last_weekly_report_day  = None
price_alerts            = {}  # {symbol: {"price": float, "direction": "above"/"below"}}

# ================= CONSTANTS =================
SCAN_INTERVAL            = 300
BATCH_INTERVAL           = 1800
RIVER_INTERVAL           = 900
MIN_SETUP_SCORE          = 95
MIN_PRIMARY_SCORE        = 90
INSTANT_SIGNAL_THRESHOLD = 97
MIN_PROFIT_TARGET        = 20.0
DELAY_BETWEEN_COINS      = 0.15
MAX_SIGNALS_PER_BATCH    = 1
MAX_ACTIVE_TRADES        = 5
SIGNAL_EXPIRY_MINUTES    = 30
INSTANT_EXPIRY_MINUTES   = 15
ATR_SL_MULTIPLIER        = 2.5
ATR_TP_MULTIPLIER        = 5.0
MAX_DAILY_LOSSES         = 3
WHALE_TRADE_THRESHOLD    = 500000
ATR_VOLATILITY_RATIO     = 3.0
CONSEC_LOSS_SUSPEND      = 3     # suspend pattern after 3 consecutive losses
BTC_CORRELATED           = ["ETH","BNB","SOL","AVAX","NEAR","APT","SUI"]

BOT_VERSION = "v32"
BOT_NAME    = "TRADING SIGNAL MASTER"
BOT_HEADER  = f"⚙️ {BOT_NAME} {BOT_VERSION}"

STARTUP_ART = f"""
╔══════════════════════════════════════╗
║   🚀 TRADING SIGNAL MASTER v32 🚀   ║
║   ─────────────────────────────────  ║
║   Smart • Fast • Accurate • Reliable ║
╚══════════════════════════════════════╝
"""

# ================= PERSISTENCE =================
def save_active_trades():
    with trade_lock:
        try:
            serializable = {
                k: {**v,
                    "timestamp": v["timestamp"].isoformat(),
                    "expires_at": v["expires_at"].isoformat() if v.get("expires_at") else None}
                for k, v in active_trades.items()
            }
            with open("active_trades.json","w") as f: json.dump(serializable,f)
        except Exception as e: logger.error(f"Save active trades failed: {e}")

def load_active_trades():
    global active_trades
    try:
        if os.path.exists("active_trades.json"):
            with open("active_trades.json","r") as f:
                data = json.load(f)
                active_trades = {
                    k: {**v,
                        "timestamp": datetime.fromisoformat(v["timestamp"]),
                        "expires_at": datetime.fromisoformat(v["expires_at"]) if v.get("expires_at") else None}
                    for k, v in data.items()
                }
            logger.info(f"Loaded {len(active_trades)} active trades.")
    except Exception as e: logger.error(f"Load active trades failed: {e}")

def save_trade_history():
    with trade_lock:
        try:
            with open("trades.json","w") as f: json.dump(pattern_stats,f)
        except Exception as e: logger.error(f"Save trade history failed: {e}")

def load_trade_history():
    global pattern_stats
    try:
        if os.path.exists("trades.json"):
            with open("trades.json","r") as f:
                loaded = json.load(f)
                for p in pattern_stats:
                    if p in loaded: pattern_stats[p] = loaded[p]
    except Exception as e: logger.error(f"Load trade history failed: {e}")

def save_journal():
    try:
        with open("journal.json","w") as f: json.dump(trade_journal,f)
    except Exception as e: logger.error(f"Save journal failed: {e}")

def load_journal():
    global trade_journal
    try:
        if os.path.exists("journal.json"):
            with open("journal.json","r") as f: trade_journal = json.load(f)
        logger.info(f"Loaded {len(trade_journal)} journal entries.")
    except Exception as e: logger.error(f"Load journal failed: {e}")

def save_learning():
    try:
        with open("learning.json","w") as f:
            json.dump({"notes":learning_notes,"memory":market_memory,
                       "consecutive_losses":consecutive_loss_patterns},f)
    except Exception as e: logger.error(f"Save learning failed: {e}")

def load_learning():
    global learning_notes, market_memory, consecutive_loss_patterns
    try:
        if os.path.exists("learning.json"):
            with open("learning.json","r") as f:
                data = json.load(f)
                learning_notes            = data.get("notes",[])
                market_memory.update(data.get("memory",{}))
                consecutive_loss_patterns = data.get("consecutive_losses",{})
    except Exception as e: logger.error(f"Load learning failed: {e}")

def save_daily_sent():
    try:
        with open("daily_sent.json","w") as f:
            json.dump({"date":str(datetime.now(IST).date()),"coins":list(daily_sent_coins)},f)
    except Exception as e: logger.error(f"Save daily sent failed: {e}")

def load_daily_sent():
    global daily_sent_coins
    try:
        if os.path.exists("daily_sent.json"):
            with open("daily_sent.json","r") as f:
                data = json.load(f)
                if data.get("date") == str(datetime.now(IST).date()):
                    daily_sent_coins = set(data.get("coins",[]))
    except Exception as e: logger.error(f"Load daily sent failed: {e}")

def save_alerts():
    try:
        with open("alerts.json","w") as f: json.dump(price_alerts,f)
    except Exception as e: logger.error(f"Save alerts failed: {e}")

def load_alerts():
    global price_alerts
    try:
        if os.path.exists("alerts.json"):
            with open("alerts.json","r") as f: price_alerts = json.load(f)
    except Exception as e: logger.error(f"Load alerts failed: {e}")

# ================= UTILS =================
def format_price(price: float) -> str:
    if price >= 1000:    return f"{price:.2f}"
    elif price >= 1:     return f"{price:.4f}"
    elif price >= 0.01:  return f"{price:.6f}"
    else:                return f"{price:.8f}"

def get_ist_time() -> str:
    return datetime.now(IST).strftime("%I:%M:%S %p IST")

def get_ist_datetime() -> datetime:
    return datetime.now(IST)

# ================= TELEGRAM =================
def send_telegram(text: str, parse_mode: str = "HTML", reply_markup: dict = None) -> bool:
    payload = {"chat_id":CHAT_ID,"text":text,"parse_mode":parse_mode}
    if reply_markup: payload["reply_markup"] = reply_markup
    try:
        res = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                            json=payload, timeout=15)
        if res.status_code != 200:
            logger.warning(f"Telegram failed [{res.status_code}]: {res.text}")
        return res.status_code == 200
    except requests.RequestException as e:
        logger.error(f"Telegram error: {e}"); return False

# ================= BINANCE =================
def get_price(symbol: str) -> float | None:
    try:
        res = requests.get(BINANCE_PRICE_URL, params={"symbol":symbol}, timeout=10)
        return float(res.json()["price"]) if res.status_code == 200 else None
    except Exception as e:
        logger.warning(f"Price failed {symbol}: {e}"); return None

def get_klines(symbol: str, interval: str, limit: int = 100) -> list:
    try:
        res = requests.get(BINANCE_KLINE_URL,
                           params={"symbol":symbol,"interval":interval,"limit":limit},
                           timeout=10)
        return res.json() if res.status_code == 200 else []
    except Exception as e:
        logger.warning(f"Klines failed {symbol}: {e}"); return []

# ================= INDICATORS =================
def calculate_ema(closes: list, period: int) -> float | None:
    if len(closes) < period: return None
    ema = sum(closes[:period]) / period
    k   = 2 / (period + 1)
    for price in closes[period:]: ema = price * k + ema * (1 - k)
    return ema

def calculate_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1: return 50
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(0,diff)); losses.append(max(0,-diff))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    return 100 - (100/(1+ag/al)) if al != 0 else 100

def calculate_atr(klines: list, period: int = 14) -> float:
    if len(klines) < period + 1: return 0
    trs = []
    for i in range(1, len(klines)):
        h=float(klines[i][2]); l=float(klines[i][3]); pc=float(klines[i-1][4])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return sum(trs[-period:]) / period

# ================= MARKET CONDITION =================
def detect_market_condition(btc_price: float, btc_klines: list) -> str:
    try:
        closes    = [float(k[4]) for k in btc_klines]
        ema20     = calculate_ema(closes, 20)
        ema50     = calculate_ema(closes, 50)
        high_20   = max(closes[-20:])
        low_20    = min(closes[-20:])
        range_pct = ((high_20-low_20)/low_20)*100 if low_20 > 0 else 0
        if ema20 and ema50:
            if ema20 > ema50*1.02 and btc_price > ema20:   return "bull"
            elif ema20 < ema50*0.98 and btc_price < ema20: return "bear"
        return "sideways" if range_pct < 5.0 else ("bull" if btc_price > (ema50 or btc_price) else "bear")
    except Exception as e:
        logger.warning(f"Market condition failed: {e}"); return "sideways"

# ================= SMART POSITION SIZING =================
def get_smart_leverage(symbol: str, atr_pct: float, score: float) -> int:
    """
    Dynamically adjusts leverage based on setup quality score.
    Higher confidence = higher leverage (within safe limits).
    """
    base = symbol.replace("USDT","")
    if base in ["BTC","ETH"]:
        base_lev = 10
    elif base in ["BNB","SOL"]:
        base_lev = 8
    elif atr_pct < 2.0:
        base_lev = 8
    elif atr_pct < 4.0:
        base_lev = 5
    else:
        base_lev = 4

    # Score-based multiplier
    if score >= 99:
        multiplier = 2.0
    elif score >= 98:
        multiplier = 1.75
    elif score >= 97:
        multiplier = 1.5
    elif score >= 96:
        multiplier = 1.25
    else:
        multiplier = 1.0

    final_lev = int(base_lev * multiplier)
    return min(final_lev, 20)  # hard cap at 20x

# ================= SIGNAL QUALITY GRADE =================
def get_signal_grade(score: float, whale: bool, oi_rising, tf_score: int,
                     volume_ok: bool, rsi_ok: bool, funding_ok: bool) -> str:
    """
    Grades each signal A/B/C based on how many filters it passed.
    A = exceptional, B = good, C = minimum qualifying.
    """
    points = 0
    if score >= 98:              points += 3
    elif score >= 96:            points += 2
    else:                        points += 1
    if whale:                    points += 2
    if oi_rising:                points += 2
    if tf_score == 3:            points += 2
    elif tf_score == 2:          points += 1
    if volume_ok:                points += 1
    if rsi_ok:                   points += 1
    if funding_ok:               points += 1

    if points >= 10:   return "⭐⭐⭐ Grade A+"
    elif points >= 8:  return "⭐⭐ Grade A"
    elif points >= 6:  return "✅ Grade B"
    else:              return "⚠️ Grade C"

# ================= FILTERS =================
def is_volume_confirmed(klines: list) -> bool:
    vols = [float(k[5]) for k in klines]
    return len(vols) >= 20 and vols[-1] > sum(vols[-20:])/20 * 1.1

def is_rsi_valid(closes: list, direction: str) -> bool:
    rsi = calculate_rsi(closes)
    return not (direction=="BUY" and rsi>72) and not (direction=="SELL" and rsi<28)

def is_volatility_normal(klines: list) -> bool:
    atr_now=calculate_atr(klines,14); atr_slow=calculate_atr(klines,50)
    return atr_slow==0 or (atr_now/atr_slow) <= ATR_VOLATILITY_RATIO

def is_pattern_blacklisted(pattern_name: str) -> bool:
    s = pattern_stats.get(pattern_name)
    if not s or s["signals"] < 10: return False
    return (s["wins"]/s["signals"])*100 < 40

def is_pattern_suspended(pattern_name: str) -> bool:
    """Temporarily suspends pattern after 3 consecutive losses."""
    data = consecutive_loss_patterns.get(pattern_name, {})
    if data.get("consecutive_losses",0) >= CONSEC_LOSS_SUSPEND:
        suspended_until = data.get("suspended_until")
        if suspended_until:
            try:
                su = datetime.fromisoformat(suspended_until)
                if datetime.now(IST) < su:
                    logger.info(f"Pattern {pattern_name} suspended until {su}")
                    return True
                else:
                    consecutive_loss_patterns[pattern_name]["consecutive_losses"] = 0
                    consecutive_loss_patterns[pattern_name]["suspended_until"]    = None
            except Exception: pass
    return False

def too_many_correlated_active() -> bool:
    return sum(1 for c in active_trades if c in BTC_CORRELATED) >= 2

# ================= FUNDING RATE =================
def get_funding_rate(symbol: str) -> float | None:
    try:
        res = requests.get(BINANCE_FUNDING_URL,
                           params={"symbol":symbol,"limit":1}, timeout=10)
        return float(res.json()[0]["fundingRate"]) if res.status_code==200 and res.json() else None
    except Exception as e:
        logger.warning(f"Funding failed {symbol}: {e}"); return None

def is_funding_favorable(symbol: str, direction: str) -> bool:
    rate = get_funding_rate(symbol)
    if rate is None: return True
    if direction=="BUY"  and rate >  0.002: return False
    if direction=="SELL" and rate < -0.002: return False
    return True

# ================= OPEN INTEREST =================
def get_oi_trend(symbol: str) -> bool | None:
    try:
        res = requests.get(BINANCE_OI_URL,
                           params={"symbol":symbol,"period":"15m","limit":5}, timeout=10)
        if res.status_code==200 and len(res.json())>=2:
            d=res.json()
            return float(d[-1]["sumOpenInterest"]) > float(d[-2]["sumOpenInterest"])
        return None
    except Exception as e:
        logger.warning(f"OI failed {symbol}: {e}"); return None

# ================= WHALE =================
def has_whale_activity(symbol: str) -> bool:
    try:
        res = requests.get(BINANCE_AGG_URL,
                           params={"symbol":symbol,"limit":20}, timeout=10)
        if res.status_code==200:
            for t in res.json():
                if float(t["p"])*float(t["q"]) > WHALE_TRADE_THRESHOLD: return True
        return False
    except Exception as e:
        logger.warning(f"Whale failed {symbol}: {e}"); return False

# ================= FEAR & GREED =================
def get_fear_greed_index() -> int:
    try:
        res = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        return int(res.json()["data"][0]["value"]) if res.status_code==200 else 50
    except Exception as e:
        logger.warning(f"F&G failed: {e}"); return 50

def is_sentiment_valid(direction: str, fng: int) -> bool:
    return not (direction=="BUY" and fng<20) and not (direction=="SELL" and fng>80)

# ================= HTF TREND =================
def get_htf_trend(symbol: str, interval: str = "1h") -> int:
    try:
        klines = get_klines(symbol, interval, 50)
        if not klines or len(klines)<50: return 0
        closes = [float(k[4]) for k in klines]
        ema20  = calculate_ema(closes,20)
        ema50  = calculate_ema(closes,50)
        if ema20 and ema50: return 1 if ema20>ema50 else -1
        return 0
    except Exception as e:
        logger.warning(f"HTF failed {symbol} {interval}: {e}"); return 0

def get_timeframe_score(symbol: str, direction: str) -> int:
    direction_int = 1 if direction=="BUY" else -1
    htf_4h = get_htf_trend(symbol,"4h")
    htf_1h = get_htf_trend(symbol,"1h")
    if htf_4h != 0 and htf_4h != direction_int: return -1
    score = 0
    if htf_4h == direction_int: score += 2
    if htf_1h == direction_int: score += 1
    return score

# ================= STRUCTURE SL =================
def get_structure_sl(klines: list, direction: str, entry: float, atr: float) -> float:
    lows  = [float(k[3]) for k in klines[-20:]]
    highs = [float(k[2]) for k in klines[-20:]]
    if direction=="BUY":
        return min(min(lows)*0.998, entry-atr*ATR_SL_MULTIPLIER)
    return max(max(highs)*1.002, entry+atr*ATR_SL_MULTIPLIER)

# ================= CIRCUIT BREAKER =================
def check_circuit_breaker() -> bool:
    global daily_losses, last_reset_day
    today = datetime.now(IST).date()
    if today != last_reset_day:
        daily_losses=0; last_reset_day=today
    return daily_losses >= MAX_DAILY_LOSSES

def increment_daily_losses():
    global daily_losses
    daily_losses += 1
    logger.warning(f"Daily losses: {daily_losses}/{MAX_DAILY_LOSSES}")
    if daily_losses >= MAX_DAILY_LOSSES:
        send_telegram(
            f"🚨 <b>{BOT_HEADER}</b>\n\n"
            f"CIRCUIT BREAKER ACTIVE\n"
            f"3 losses today. No more signals until tomorrow. 🛡️"
        )

# ================= LEARNING SYSTEM =================
def learn_from_trade(coin: str, pattern: str, result: str,
                     pnl: float, market_condition: str, tf_score: int):
    global learning_notes, market_memory, consecutive_loss_patterns

    # Update market memory
    if result=="WIN": market_memory[market_condition]["wins"]   += 1
    else:             market_memory[market_condition]["losses"] += 1

    # Best pattern per condition
    wins_by_pattern = {}
    for entry in trade_journal:
        if entry.get("market_condition")==market_condition and entry.get("result")=="WIN":
            p=entry.get("pattern","Unknown")
            wins_by_pattern[p] = wins_by_pattern.get(p,0)+1
    if wins_by_pattern:
        market_memory[market_condition]["best_pattern"] = max(wins_by_pattern,key=wins_by_pattern.get)

    # Consecutive loss tracking
    if pattern not in consecutive_loss_patterns:
        consecutive_loss_patterns[pattern] = {"consecutive_losses":0,"suspended_until":None}

    if result=="LOSS":
        consecutive_loss_patterns[pattern]["consecutive_losses"] += 1
        cl = consecutive_loss_patterns[pattern]["consecutive_losses"]
        if cl >= CONSEC_LOSS_SUSPEND:
            su = (datetime.now(IST)+timedelta(hours=24)).isoformat()
            consecutive_loss_patterns[pattern]["suspended_until"] = su
            send_telegram(
                f"🧠 <b>{BOT_HEADER} Pattern Suspended</b>\n\n"
                f"'{pattern}' has {cl} consecutive losses.\n"
                f"Suspended for 24 hours automatically. 🔒"
            )
    else:
        consecutive_loss_patterns[pattern]["consecutive_losses"] = 0
        consecutive_loss_patterns[pattern]["suspended_until"]    = None

    # Generate learning notes
    stats   = pattern_stats.get(pattern,{})
    signals = stats.get("signals",0)
    note    = None

    if signals >= 5:
        win_rate = (stats["wins"]/signals)*100
        if result=="LOSS" and win_rate < 45:
            note = (f"⚠️ '{pattern}' only {win_rate:.1f}% WR after {signals} signals "
                    f"in {market_condition} market. Consider avoiding.")
        elif result=="WIN" and win_rate > 70:
            note = (f"✅ '{pattern}' performing strong — {win_rate:.1f}% WR "
                    f"in {market_condition} market. Keep prioritising.")
        elif result=="LOSS" and market_condition=="sideways":
            note = (f"📊 Loss on '{pattern}' in sideways market. "
                    f"Struggles when BTC ranges. Reduce size in sideways.")
        elif tf_score < 3 and result=="LOSS":
            note = (f"📉 Loss on {coin} with TF score {tf_score}/3. "
                    f"Losses higher when 4h and 1h don't both confirm.")

    if note and note not in learning_notes:
        learning_notes.append(note)
        if len(learning_notes)>100: learning_notes = learning_notes[-100:]
        if "⚠️" in note or "📉" in note:
            send_telegram(f"🧠 <b>{BOT_HEADER} Auto Learning Alert</b>\n\n{note}")
        logger.info(f"Learning note: {note}")

    save_learning()

def generate_weekly_insight() -> str:
    """Analyzes last 7 days and writes smart insight message."""
    today = datetime.now(IST).date()
    week_trades = [j for j in trade_journal
                   if (today-datetime.strptime(j["date"],"%Y-%m-%d").date()).days < 7]

    if not week_trades:
        return "Not enough data for weekly insight."

    wins   = [t for t in week_trades if t["result"]=="WIN"]
    losses = [t for t in week_trades if t["result"]=="LOSS"]
    total  = len(week_trades)
    wr     = (len(wins)/total*100) if total>0 else 0

    # Best day
    day_wins = {}
    for t in wins:
        d=t["date"]; day_wins[d]=day_wins.get(d,0)+1
    best_day = max(day_wins,key=day_wins.get) if day_wins else None

    # Pattern analysis
    win_pats  = [t["pattern"] for t in wins]
    loss_pats = [t["pattern"] for t in losses]
    best_pat  = Counter(win_pats).most_common(1)[0][0]  if win_pats  else None
    worst_pat = Counter(loss_pats).most_common(1)[0][0] if loss_pats else None

    # Market condition analysis
    sideways_losses = sum(1 for t in losses if t.get("market_condition")=="sideways")

    msg  = f"🧠 <b>{BOT_HEADER} Weekly AI Insight</b>\n\n"
    msg += f"📊 {len(wins)}W / {len(losses)}L | {wr:.1f}% WR\n\n"
    if best_day:
        msg += f"📅 Best day: {best_day}\n"
    if best_pat:
        msg += f"⭐ Best pattern: {best_pat}\n"
    if worst_pat:
        msg += f"⚠️ Most losses from: {worst_pat}\n"
    if sideways_losses >= 2:
        msg += f"📊 {sideways_losses} losses in sideways market — reduce activity when BTC ranges\n"
    if wr >= 70:
        msg += "\n🔥 Excellent week! Bot is performing strongly."
    elif wr >= 50:
        msg += "\n✅ Decent week. Keep following the signals."
    else:
        msg += "\n⚠️ Tough week. Review learning notes and be patient."
    return msg

def get_learning_text() -> str:
    if not learning_notes:
        return f"🧠 <b>{BOT_HEADER} Learning</b>\n\nNo insights yet. Need more trade data."
    text  = f"🧠 <b>{BOT_HEADER} Bot Learning Insights</b>\n\n"
    text += "<b>Market Memory:</b>\n"
    for cond in ["bull","bear","sideways"]:
        mem  = market_memory[cond]
        tot  = mem["wins"]+mem["losses"]
        wr   = (mem["wins"]/tot*100) if tot>0 else 0
        best = mem["best_pattern"] or "N/A"
        text += f"{cond.capitalize()}: {mem['wins']}W/{mem['losses']}L ({wr:.1f}%) | Best: {best}\n"
    text += f"\n<b>Suspended Patterns:</b>\n"
    suspended = [p for p,d in consecutive_loss_patterns.items() if d.get("consecutive_losses",0)>=CONSEC_LOSS_SUSPEND and d.get("suspended_until")]
    text += "\n".join([f"🔒 {p}" for p in suspended]) if suspended else "None\n"
    text += f"\n\n<b>Latest Insights ({len(learning_notes)}):</b>\n\n"
    for note in learning_notes[-10:]: text += f"• {note}\n\n"
    return text

# ================= NEWS =================
def get_crypto_news() -> str:
    items = []
    if NEWS_API_KEY:
        try:
            res = requests.get("https://cryptopanic.com/api/v1/posts/",
                               params={"auth_token":NEWS_API_KEY,"kind":"news","filter":"hot"},
                               timeout=10)
            if res.status_code==200:
                for item in res.json().get("results",[])[:5]:
                    items.append(f"📰 {item['title'][:80]}")
        except Exception as e: logger.warning(f"CryptoPanic failed: {e}")

    try:
        res = requests.get("https://api.alternative.me/fng/?limit=3", timeout=10)
        if res.status_code==200:
            data=res.json()["data"]; latest=data[0]
            yesterday=data[1] if len(data)>1 else None
            fng_val=int(latest["value"]); fng_cls=latest["value_classification"]
            change=""
            if yesterday:
                diff=fng_val-int(yesterday["value"])
                change=f" ({'+' if diff>=0 else ''}{diff} vs yesterday)"
            items.append(f"😱 Fear & Greed: {fng_val} — {fng_cls}{change}")
    except Exception as e: logger.warning(f"F&G news failed: {e}")

    try:
        btc=get_price("BTCUSDT"); eth=get_price("ETHUSDT"); sol=get_price("SOLUSDT")
        bnb=get_price("BNBUSDT"); xrp=get_price("XRPUSDT")
        if btc: items.append(f"₿  BTC:  ${btc:>12,.2f}")
        if eth: items.append(f"Ξ  ETH:  ${eth:>12,.2f}")
        if sol: items.append(f"◎  SOL:  ${sol:>12,.2f}")
        if bnb: items.append(f"🔶 BNB:  ${bnb:>12,.2f}")
        if xrp: items.append(f"✦  XRP:  ${xrp:>12,.4f}")
    except Exception as e: logger.warning(f"Price snap failed: {e}")

    if not items:
        return f"📰 <b>{BOT_HEADER} Market News</b>\n\nNo news available. Try again shortly."
    msg  = f"📰 <b>{BOT_HEADER} Market Update — {get_ist_time()}</b>\n\n"
    msg += "\n\n".join(items)
    msg += f"\n\n🔄 Refreshed: {get_ist_time()}"
    return msg

# ================= BACKTESTING =================
def run_backtest(symbol: str) -> str:
    try:
        klines=get_klines(symbol,"15m",1000)
        if not klines or len(klines)<100:
            return f"Not enough data for {symbol}"

        results={"WIN":0,"LOSS":0,"SKIP":0}
        pattern_res={}
        cond_res={"bull":{"W":0,"L":0},"bear":{"W":0,"L":0},"sideways":{"W":0,"L":0}}
        total_pnl=0.0; window=60

        for i in range(window, len(klines)-10):
            wk=klines[i-window:i]; price=float(klines[i][4])
            closes=[float(k[4]) for k in wk]
            ema20=calculate_ema(closes,20); ema50=calculate_ema(closes,50)
            rng=((max(closes[-20:])-min(closes[-20:]))/min(closes[-20:]))*100 if min(closes[-20:])>0 else 0
            if ema20 and ema50:
                if ema20>ema50*1.02:   cond="bull"
                elif ema20<ema50*0.98: cond="bear"
                else:                  cond="sideways" if rng<5 else ("bull" if price>ema50 else "bear")
            else: cond="sideways"

            btc_trend=1 if (ema20 and ema50 and ema20>ema50) else -1
            found=detect_patterns(symbol,wk,price,btc_trend)
            if not found: continue

            best=max(found,key=lambda x:x[1])
            if best[1]<MIN_PRIMARY_SCORE: continue

            atr=calculate_atr(wk)
            if atr==0: continue

            entry=price; direction=best[2]
            sl=entry-(atr*ATR_SL_MULTIPLIER) if direction=="BUY" else entry+(atr*ATR_SL_MULTIPLIER)
            tp=entry+(atr*ATR_TP_MULTIPLIER) if direction=="BUY" else entry-(atr*ATR_TP_MULTIPLIER)

            hit="SKIP"
            for j in range(i+1, min(i+96,len(klines))):
                fh=float(klines[j][2]); fl=float(klines[j][3])
                if direction=="BUY":
                    if fh>=tp: hit="WIN";  break
                    if fl<=sl: hit="LOSS"; break
                else:
                    if fl<=tp: hit="WIN";  break
                    if fh>=sl: hit="LOSS"; break

            if hit=="SKIP": results["SKIP"]+=1; continue
            results[hit]+=1
            cond_res[cond]["W" if hit=="WIN" else "L"]+=1
            pnl=(abs(tp-entry)/entry)*100*5 if hit=="WIN" else -(abs(sl-entry)/entry)*100*5
            total_pnl+=pnl
            pat=best[0]
            if pat not in pattern_res: pattern_res[pat]={"W":0,"L":0}
            pattern_res[pat]["W" if hit=="WIN" else "L"]+=1

        total=results["WIN"]+results["LOSS"]
        wr=(results["WIN"]/total*100) if total>0 else 0

        r  = f"📊 <b>{BOT_HEADER} Backtest: {symbol}</b>\n\n"
        r += f"Candles: {len(klines)} x 15m\n"
        r += f"Trades: {total} | Skipped: {results['SKIP']}\n"
        r += f"✅ Wins: {results['WIN']} | ❌ Losses: {results['LOSS']}\n"
        r += f"🎯 Win Rate: {wr:.1f}%\n"
        r += f"💰 Sim PnL: {total_pnl:+.1f}%\n\n"
        r += "<b>By Market Condition:</b>\n"
        for cond,res in cond_res.items():
            ct=res["W"]+res["L"]
            wr2=(res["W"]/ct*100) if ct>0 else 0
            r+=f"{cond.capitalize()}: {res['W']}W/{res['L']}L ({wr2:.1f}%)\n"
        r+="\n<b>Top Patterns:</b>\n"
        for pat,res in sorted(pattern_res.items(),key=lambda x:x[1]["W"],reverse=True)[:5]:
            ct=res["W"]+res["L"]
            wr2=(res["W"]/ct*100) if ct>0 else 0
            r+=f"{pat}: {wr2:.1f}% ({ct} trades)\n"
        return r
    except Exception as e:
        logger.error(f"Backtest failed {symbol}: {e}",exc_info=True)
        return f"Backtest failed: {e}"

# ================= TEXT HELPERS =================
def get_active_trades_text() -> str:
    if not active_trades: return f"📊 <b>{BOT_HEADER}</b>\nNo active trades."
    text=f"📊 <b>{BOT_HEADER} Active Trades ({len(active_trades)})</b>\n\n"
    for coin,t in active_trades.items():
        text+=f"<b>{coin}</b> {t['direction']}\n"
        text+=f"Entry: {format_price(t['entry'])} | SL: {format_price(t['sl'])}\n"
        text+=f"TP: {format_price(t['tp'])} | Lev: {t['leverage']}x\n\n"
    return text

def get_pattern_stats_text() -> str:
    text=f"📈 <b>{BOT_HEADER} Pattern Performance</b>\n\n"
    for pat,s in sorted(pattern_stats.items(),key=lambda x:x[1]["signals"],reverse=True)[:10]:
        if s["signals"]>0:
            wr=(s["wins"]/s["signals"])*100
            flag="🔴" if wr<40 else "🟡" if wr<60 else "🟢"
            susp=" 🔒" if is_pattern_suspended(pat) else ""
            text+=f"{flag} <b>{pat}</b>{susp}\nSignals: {s['signals']} | Win: {wr:.1f}% | PnL: {s['total_pnl']:.1f}%\n\n"
    return text

def get_10day_summary_text() -> str:
    today  = datetime.now(IST).date()
    text   = f"📅 <b>{BOT_HEADER} Last 10 Days Summary</b>\n\n"
    overall_wins=overall_losses=0; overall_pnl=0.0

    for days_ago in range(9,-1,-1):
        day        = today-timedelta(days=days_ago)
        day_trades = [j for j in trade_journal if j.get("date")==str(day)]
        wins       = sum(1 for t in day_trades if t["result"]=="WIN")
        losses     = sum(1 for t in day_trades if t["result"]=="LOSS")
        total      = wins+losses
        pnl        = sum(t["pnl"] for t in day_trades)
        wr         = (wins/total*100) if total>0 else 0
        day_str    = day.strftime("%d %b")
        overall_wins+=wins; overall_losses+=losses; overall_pnl+=pnl
        if total==0:
            text+=f"<b>{day_str}</b> — No trades\n"
        else:
            emoji="✅" if wins>losses else "❌" if losses>wins else "➖"
            text+=f"{emoji} <b>{day_str}</b>: {wins}W/{losses}L | WR:{wr:.0f}% | PnL:{pnl:+.1f}%\n"

    overall_total=overall_wins+overall_losses
    overall_wr=(overall_wins/overall_total*100) if overall_total>0 else 0
    text+=f"\n<b>━━━ 10 Day Total ━━━</b>\n"
    text+=f"✅ {overall_wins}W | ❌ {overall_losses}L\n"
    text+=f"🎯 Win Rate: {overall_wr:.1f}%\n"
    text+=f"💰 Total PnL: {overall_pnl:+.2f}%\n"
    text+=f"📊 Avg PnL/day: {overall_pnl/10:+.2f}%"
    return text

def get_streak_text() -> str:
    if not trade_journal:
        return f"🔥 <b>{BOT_HEADER} Streak</b>\n\nNo trades yet."
    streak_type=trade_journal[-1]["result"]; streak_count=0
    for t in reversed(trade_journal):
        if t["result"]==streak_type: streak_count+=1
        else: break
    emoji="🔥" if streak_type=="WIN" else "❄️"
    text=f"{emoji} <b>{BOT_HEADER} Current Streak</b>\n\n"
    text+=f"{'Winning' if streak_type=='WIN' else 'Losing'} streak: <b>{streak_count}</b> trades\n"
    if streak_type=="WIN" and streak_count>=3:
        text+="\n🔥 You're on fire! Stay disciplined."
    elif streak_type=="LOSS" and streak_count>=2:
        text+="\n⚠️ Losing streak. Consider reducing size or waiting."
    return text

def get_best_text() -> str:
    if not trade_journal:
        return f"⭐ <b>{BOT_HEADER} Best Performers</b>\n\nNo trade data yet."
    coin_stats={}; pattern_stats2={}
    for t in trade_journal:
        c=t["coin"]
        if c not in coin_stats: coin_stats[c]={"W":0,"L":0,"pnl":0}
        coin_stats[c]["W" if t["result"]=="WIN" else "L"]+=1
        coin_stats[c]["pnl"]+=t["pnl"]
        p=t["pattern"]
        if p not in pattern_stats2: pattern_stats2[p]={"W":0,"L":0}
        pattern_stats2[p]["W" if t["result"]=="WIN" else "L"]+=1

    text=f"⭐ <b>{BOT_HEADER} Best Performers (All Time)</b>\n\n"
    text+="<b>Top Coins:</b>\n"
    sc=sorted(coin_stats.items(),
              key=lambda x:(x[1]["W"]/(x[1]["W"]+x[1]["L"]) if (x[1]["W"]+x[1]["L"])>0 else 0),
              reverse=True)[:3]
    for i,(coin,s) in enumerate(sc,1):
        tot=s["W"]+s["L"]; wr=(s["W"]/tot*100) if tot>0 else 0
        text+=f"{i}. <b>{coin}</b> — {wr:.1f}% WR | PnL: {s['pnl']:+.1f}%\n"

    text+="\n<b>Top Patterns:</b>\n"
    sp=sorted(pattern_stats2.items(),
              key=lambda x:(x[1]["W"]/(x[1]["W"]+x[1]["L"]) if (x[1]["W"]+x[1]["L"])>0 else 0),
              reverse=True)[:3]
    for i,(pat,s) in enumerate(sp,1):
        tot=s["W"]+s["L"]; wr=(s["W"]/tot*100) if tot>0 else 0
        text+=f"{i}. <b>{pat}</b> — {wr:.1f}% WR ({tot} trades)\n"
    return text

def get_risk_text() -> str:
    if not active_trades: return f"🛡️ <b>{BOT_HEADER} Risk</b>\n\nNo active trades."
    text=f"🛡️ <b>{BOT_HEADER} Risk Exposure</b>\n\n"
    total_risk=0
    for coin,t in active_trades.items():
        risk_pct=abs(t["entry"]-t["sl"])/t["entry"]*100*t["leverage"]
        total_risk+=risk_pct
        text+=f"<b>{coin}</b>: {risk_pct:.1f}% max loss at SL\n"
    text+=f"\n📊 Total Risk: {total_risk:.1f}%\n"
    text+=f"📌 Active: {len(active_trades)}/{MAX_ACTIVE_TRADES}\n"
    text+=f"🛡️ Circuit Breaker: {'🔴 ACTIVE' if check_circuit_breaker() else '🟢 OK'}"
    return text

def expire_pending_signals():
    now=get_ist_datetime()
    expired=[c for c,s in list(pending_signals.items())
             if s.get("expires_at") and now>s["expires_at"]]
    for coin in expired:
        del pending_signals[coin]
        send_telegram(f"⏰ <b>{BOT_HEADER}</b> Signal expired: <b>{coin}</b>")
        logger.info(f"Expired: {coin}")

def check_price_alerts():
    """Checks all active price alerts and fires if triggered."""
    triggered=[]
    for symbol, alert in list(price_alerts.items()):
        price=get_price(symbol+"USDT")
        if not price: continue
        if alert["direction"]=="above" and price >= alert["price"]:
            send_telegram(
                f"🔔 <b>{BOT_HEADER} PRICE ALERT</b>\n\n"
                f"<b>{symbol}</b> is now ABOVE {format_price(alert['price'])}\n"
                f"Current: {format_price(price)}"
            )
            triggered.append(symbol)
        elif alert["direction"]=="below" and price <= alert["price"]:
            send_telegram(
                f"🔔 <b>{BOT_HEADER} PRICE ALERT</b>\n\n"
                f"<b>{symbol}</b> is now BELOW {format_price(alert['price'])}\n"
                f"Current: {format_price(price)}"
            )
            triggered.append(symbol)
    for symbol in triggered:
        del price_alerts[symbol]
    if triggered: save_alerts()

# ================= PATTERN DETECTION =================
def detect_patterns(symbol: str, klines: list, price: float, btc_trend: int) -> list:
    if len(klines)<50: return []
    closes=[float(k[4]) for k in klines]; opens=[float(k[1]) for k in klines]
    highs=[float(k[2]) for k in klines];  lows=[float(k[3]) for k in klines]
    vols=[float(k[5]) for k in klines];   avg_vol=sum(vols[-20:])/20
    rsi=calculate_rsi(closes); ema20=calculate_ema(closes,20); ema50=calculate_ema(closes,50)

    if ((max(highs[-20:])-min(lows[-20:]))/price)*100 < 1.8: return []

    p=[]; sup=min(lows[-30:-1]); res=max(highs[-30:-1])

    if ema20 and closes[-1]>highs[-2] and closes[-2]>highs[-3] and price>ema20 and btc_trend==1:
        p.append(("Bull Flag Break",94,"BUY"))
    if ema20 and closes[-1]<lows[-2] and closes[-2]<lows[-3] and price<ema20 and btc_trend==-1:
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
        if price>ema20>ema50 and btc_trend==1:   p.append(("EMA Trend",85,"BUY"))
        elif price<ema20<ema50 and btc_trend==-1: p.append(("EMA Trend",85,"SELL"))
    if ema20 and abs(price-ema20)/ema20<0.005:
        p.append(("Pullback to 20 EMA",82,"BUY" if price>ema20 else "SELL"))
    if rsi<30:   p.append(("RSI Reversal",80,"BUY"))
    elif rsi>70: p.append(("RSI Reversal",80,"SELL"))
    mom=(closes[-1]-closes[-3])/closes[-3]*100 if len(closes)>3 else 0
    if mom>3 and btc_trend==1:   p.append(("Momentum Surge",87,"BUY"))
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

# ================= TRAILING SL =================
def update_trailing_sl(coin: str, trade: dict, price: float):
    trail=abs(trade["tp"]-trade["entry"])*0.3
    if trade["direction"]=="BUY":
        new_sl=price-trail
        if new_sl>trade["sl"]: active_trades[coin]["sl"]=new_sl; save_active_trades()
    else:
        new_sl=price+trail
        if new_sl<trade["sl"]: active_trades[coin]["sl"]=new_sl; save_active_trades()

# ================= PARTIAL TP =================
def check_partial_tp(coin: str, trade: dict, price: float, pnl: float):
    if trade.get("partial_tp_taken"): return
    halfway=((abs(trade["tp"]-trade["entry"])/2)/trade["entry"])*100*trade["leverage"]
    if pnl>=halfway:
        active_trades[coin]["partial_tp_taken"]=True
        active_trades[coin]["sl"]=trade["entry"]
        save_active_trades()
        send_telegram(
            f"💰 <b>{BOT_HEADER} PARTIAL TP: {coin}</b>\n"
            f"50% closed at {format_price(price)}\n"
            f"SL → entry 🎯 | PnL: {pnl:+.2f}%"
        )

# ================= FORMAT AND SEND =================
def get_news_headlines(coin: str) -> list:
    if not NEWS_API_KEY: return []
    try:
        res=requests.get("https://cryptopanic.com/api/v1/posts/",
                         params={"auth_token":NEWS_API_KEY,"currencies":coin,"kind":"news"},
                         timeout=5)
        return [p["title"] for p in res.json().get("results",[])[:3]]
    except Exception as e:
        logger.warning(f"Headlines failed {coin}: {e}"); return []

def format_and_send(setup: dict, coin: str, is_river: bool = False,
                    is_instant: bool = False, market_condition: str = "bull") -> bool:
    global pending_signals, sent_coins, daily_sent_coins

    if check_circuit_breaker(): return False
    if coin in daily_sent_coins and not is_instant:
        logger.info(f"{coin} already sent today — skipping duplicate"); return False

    live_price=get_price(setup["symbol"])
    if not live_price: return False
    entry=live_price

    if abs(entry-setup["scan_price"])/setup["scan_price"]>0.005:
        logger.info(f"{coin} rejected — drifted"); return False

    klines=get_klines(setup["symbol"],"15m")
    if not klines: return False

    closes=[float(x[4]) for x in klines]
    atr=calculate_atr(klines); atr_pct=(atr/entry)*100 if entry>0 else 0

    vol_ok     = is_volume_confirmed(klines)
    rsi_ok     = is_rsi_valid(closes,setup["direction"])
    funding_ok = is_funding_favorable(setup["symbol"],setup["direction"])

    if not vol_ok:     return False
    if not rsi_ok:     return False
    if not is_volatility_normal(klines): return False
    if not funding_ok: return False

    oi_rising=get_oi_trend(setup["symbol"])
    oi_label="✅ Rising" if oi_rising else "⚠️ Falling" if oi_rising is False else "➖ N/A"
    whale=has_whale_activity(setup["symbol"])

    tf_score=setup.get("tf_score",get_timeframe_score(setup["symbol"],setup["direction"]))
    lev=get_smart_leverage(setup["symbol"],atr_pct,setup["setup_score"])
    sl=get_structure_sl(klines,setup["direction"],entry,atr)
    tp=entry+(atr*ATR_TP_MULTIPLIER) if setup["direction"]=="BUY" else entry-(atr*ATR_TP_MULTIPLIER)

    profit_target=(abs(tp-entry)/entry)*100*lev
    if profit_target<MIN_PROFIT_TARGET:
        risk=abs(tp-entry)/entry
        if risk>0:
            needed=int(MIN_PROFIT_TARGET/(risk*100))+1
            if needed<=20: lev=needed; profit_target=(abs(tp-entry)/entry)*100*lev
            else: return False

    setup["leverage"]=lev
    price_range=(max(closes[-10:])-min(closes[-10:]))/10
    eta=int(abs(tp-entry)/(price_range if price_range>0 else 0.001)*15)
    expiry_minutes=INSTANT_EXPIRY_MINUTES if is_instant else SIGNAL_EXPIRY_MINUTES
    expiry_time=get_ist_datetime()+timedelta(minutes=expiry_minutes)
    expiry_str=expiry_time.strftime("%I:%M %p IST")

    mom=(closes[-1]-closes[-3])/closes[-3]*100
    rsi_val=calculate_rsi(closes)
    news=get_news_headlines(coin)

    tf_map={3:"4h✅ 1h✅ 💪 STRONG",2:"4h✅ 1h⚠️ ✅ GOOD",1:"4h⚠️ 1h✅ ⚠️ MOD",0:"4h⚠️ 1h⚠️ 🔴 COUNTER"}
    tf_label=tf_map.get(tf_score,"N/A")
    cond_label={"bull":"📈 Bull","bear":"📉 Bear","sideways":"➡️ Sideways"}.get(market_condition,"")
    grade=get_signal_grade(setup["setup_score"],whale,oi_rising,tf_score,vol_ok,rsi_ok,funding_ok)

    if is_instant: header=f"⚡ <b>{BOT_HEADER} INSTANT {coin}</b>"
    elif is_river: header=f"🌊 <b>{BOT_HEADER} RIVER SIGNAL</b>"
    else:          header=f"🔥 <b>{BOT_HEADER} VERIFIED SETUP {coin}</b>"

    msg  = f"{header}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🏆 Score: {int(setup['setup_score'])}/100 | {grade}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"📢 {setup['direction']} | Lev: {lev}x | {cond_label}\n"
    msg += f"💰 Entry: {format_price(entry)}\n"
    msg += f"🎯 TP:    {format_price(tp)}\n"
    msg += f"🛑 SL:    {format_price(sl)}\n\n"
    msg += f"📈 Profit Target: {profit_target:.2f}%\n"
    msg += f"📌 Pattern: {setup['pattern']}\n"
    msg += f"📊 RSI: {rsi_val:.2f} | Momentum: {mom:.2f}%\n"
    msg += f"📊 TF Align: {tf_label}\n"
    msg += f"📊 OI: {oi_label} | 🐋 Whale: {'✅' if whale else '❌'}\n"
    msg += f"⏳ ETA: ~{eta}m | ⏰ Expires: {expiry_str}\n"
    msg += f"✏️ ATR: {format_price(atr)}"
    if is_instant: msg+=f"\n\n⚡ <b>INSTANT — Act within {expiry_minutes} mins!</b>"
    if news: msg+="\n\n<b>📰 News:</b>\n"+"\n".join([f"• {n[:60]}..." for n in news])
    msg+=f"\n\n⏰ {get_ist_time()}"

    setup.update({
        "entry":entry,"sl":sl,"tp":tp,
        "timestamp":get_ist_datetime(),
        "expires_at":expiry_time,
        "reversal_alerted":False,
        "breakeven_sent":False,
        "partial_tp_taken":False,
        "tf_score":tf_score,
        "market_condition":market_condition
    })
    pending_signals[coin]=setup

    reply_markup={"inline_keyboard":[[
        {"text":"✅ Activate Trade","callback_data":f"ACTIVATE_{coin}"},
        {"text":"❌ Ignore",        "callback_data":f"IGNORE_{coin}"}
    ]]}

    if send_telegram(msg,reply_markup=reply_markup):
        sent_coins.append(coin)
        daily_sent_coins.add(coin)
        save_daily_sent()
        logger.info(f"Signal: {coin}|{setup['direction']}|Score:{setup['setup_score']}|Grade:{grade}|Instant:{is_instant}")
        return True
    return False

# ================= BATCH =================
def send_hourly_batch():
    global hourly_queue, last_batch_time, sent_coins
    if not hourly_queue: return
    sorted_q=sorted(hourly_queue.values(),key=lambda x:x["setup_score"],reverse=True)
    sent_count=0
    for s in sorted_q:
        if s["coin"]=="RIVER": continue
        if sent_count>=MAX_SIGNALS_PER_BATCH: break
        if format_and_send(s,s["coin"],market_condition=s.get("market_condition","bull")):
            sent_count+=1
    for s in sorted_q:
        if s["coin"] in hourly_queue: del hourly_queue[s["coin"]]
    sent_coins=[]; last_batch_time=time.time()

# ================= ACTIVE TRADE MONITORING =================
def check_active_trades():
    for coin,trade in list(active_trades.items()):
        price=get_price(trade["symbol"])
        if not price: continue

        pnl=(((price-trade["entry"])/trade["entry"])*100*trade["leverage"]
             if trade["direction"]=="BUY"
             else ((trade["entry"]-price)/trade["entry"])*100*trade["leverage"])

        update_trailing_sl(coin,trade,price)
        check_partial_tp(coin,trade,price,pnl)

        if not trade.get("reversal_alerted",False):
            klines=get_klines(trade["symbol"],"15m",20)
            if klines:
                closes=[float(x[4]) for x in klines]
                ema20=calculate_ema(closes,20)
                if ema20:
                    reversal=((trade["direction"]=="BUY"  and price<ema20*0.995) or
                              (trade["direction"]=="SELL" and price>ema20*1.005))
                    if reversal:
                        send_telegram(f"⚠️ <b>{BOT_HEADER} REVERSAL {coin}</b>\nPrice broke EMA20.")
                        active_trades[coin]["reversal_alerted"]=True; save_active_trades()

        if not trade.get("breakeven_sent",False) and pnl>=10:
            send_telegram(f"🟡 <b>{BOT_HEADER} BREAK-EVEN {coin}</b>\n+10% hit. Move SL to entry.\nPnL: {pnl:.2f}%")
            active_trades[coin]["breakeven_sent"]=True; save_active_trades()

        hit=None
        if trade["direction"]=="BUY":
            if price>=trade["tp"]:   hit="WIN"
            elif price<=trade["sl"]: hit="LOSS"
        else:
            if price<=trade["tp"]:   hit="WIN"
            elif price>=trade["sl"]: hit="LOSS"

        if hit:
            with trade_lock:
                primary=trade["pattern"].split(" + ")[0]
                if primary in pattern_stats:
                    pattern_stats[primary]["signals"]  +=1
                    pattern_stats[primary]["total_pnl"]+=pnl
                    pattern_stats[primary]["wins" if hit=="WIN" else "losses"]+=1
                if hit=="LOSS": increment_daily_losses()

                duration=""
                if trade.get("timestamp"):
                    mins=int((get_ist_datetime()-trade["timestamp"]).total_seconds()/60)
                    duration=f"{mins} mins"

                mc=trade.get("market_condition","bull")
                trade_journal.append({
                    "date":str(datetime.now(IST).date()),
                    "coin":coin,"direction":trade["direction"],
                    "pattern":primary,"entry":trade["entry"],
                    "exit":price,"pnl":pnl,"result":hit,
                    "duration":duration,"tf_score":trade.get("tf_score",0),
                    "market_condition":mc
                })
                save_journal()
                learn_from_trade(coin,primary,hit,pnl,mc,trade.get("tf_score",0))

            send_telegram(
                f"{'✅' if hit=='WIN' else '🛑'} <b>{BOT_HEADER} {coin} {hit}</b>\n\n"
                f"Entry: {format_price(trade['entry'])} → Exit: {format_price(price)}\n"
                f"Pattern: {primary} | Duration: {duration}\nPnL: {pnl:+.2f}%"
            )
            del active_trades[coin]; save_active_trades(); save_trade_history()
            logger.info(f"Closed: {coin}|{hit}|{pnl:.2f}%")

# ================= TELEGRAM POLLING =================
def poll_telegram():
    global last_update_id
    while True:
        try:
            params={}
            if last_update_id is not None: params["offset"]=last_update_id+1
            res=requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                             params=params,timeout=15)
            if res.status_code!=200: time.sleep(2); continue

            for update in res.json().get("result",[]):
                last_update_id=update["update_id"]

                if "callback_query" in update:
                    cb=update["callback_query"]; data=cb["data"]; coin=data.split("_")[1]
                    try:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                                      json={"callback_query_id":cb["id"],"text":"Processing..."},timeout=15)
                    except Exception as e: logger.warning(f"Callback failed: {e}")

                    if "ACTIVATE" in data and coin in pending_signals:
                        lp=get_price(pending_signals[coin]["symbol"])
                        if lp: pending_signals[coin]["entry"]=lp
                        with trade_lock:
                            pending_signals[coin]["breakeven_sent"]=False
                            pending_signals[coin]["partial_tp_taken"]=False
                            active_trades[coin]=pending_signals[coin]
                        save_active_trades()
                        send_telegram(f"🚀 <b>{BOT_HEADER} {coin} Activated!</b>\nEntry: {format_price(pending_signals[coin]['entry'])}")
                        del pending_signals[coin]
                    elif "IGNORE" in data and coin in pending_signals:
                        send_telegram(f"❌ <b>{BOT_HEADER}</b> {coin} Ignored")
                        del pending_signals[coin]

                elif "message" in update:
                    text=update["message"].get("text","").strip().lower()
                    if   text=="/stats":   send_telegram(get_pattern_stats_text())
                    elif text=="/trades":  send_telegram(get_active_trades_text())
                    elif text=="/summary": send_telegram(get_10day_summary_text())
                    elif text=="/streak":  send_telegram(get_streak_text())
                    elif text=="/best":    send_telegram(get_best_text())
                    elif text=="/risk":    send_telegram(get_risk_text())
                    elif text=="/learn":   send_telegram(get_learning_text())
                    elif text=="/news":
                        send_telegram("🔄 Fetching latest market news...")
                        send_telegram(get_crypto_news())
                    elif text=="/pending":
                        if pending_signals:
                            msg=f"⏳ <b>{BOT_HEADER} Pending ({len(pending_signals)})</b>\n\n"
                            for c,s in pending_signals.items():
                                exp=s.get("expires_at")
                                msg+=f"<b>{c}</b> {s['direction']} | Score:{s['setup_score']} | Exp:{exp.strftime('%I:%M %p') if exp else 'N/A'}\n"
                            send_telegram(msg)
                        else: send_telegram(f"<b>{BOT_HEADER}</b> No pending signals.")
                    elif text.startswith("/backtest"):
                        parts=text.split(); bc=(parts[1].upper() if len(parts)>1 else "BTC")+"USDT"
                        send_telegram(f"🔄 Running backtest for {bc}...")
                        send_telegram(run_backtest(bc))
                    elif text.startswith("/alert"):
                        # Usage: /alert BTC 95000 above  OR  /alert BTC 90000 below
                        parts=text.split()
                        if len(parts)>=4:
                            try:
                                sym=parts[1].upper(); target=float(parts[2]); direction=parts[3].lower()
                                price_alerts[sym]={"price":target,"direction":direction}
                                save_alerts()
                                send_telegram(f"🔔 Alert set: <b>{sym}</b> {direction} {format_price(target)}")
                            except Exception as e:
                                send_telegram(f"❌ Invalid alert format. Use: /alert BTC 95000 above")
                        else:
                            send_telegram("❌ Usage: /alert BTC 95000 above")
                    elif text=="/alerts":
                        if price_alerts:
                            msg=f"🔔 <b>{BOT_HEADER} Active Alerts</b>\n\n"
                            for sym,a in price_alerts.items():
                                msg+=f"<b>{sym}</b>: {a['direction']} {format_price(a['price'])}\n"
                            send_telegram(msg)
                        else: send_telegram(f"<b>{BOT_HEADER}</b> No active alerts.")
                    elif text=="/help":
                        msg  = f"📖 <b>{BOT_HEADER} Commands</b>\n\n"
                        msg += "/trades    — Active trades\n"
                        msg += "/pending   — Pending signals\n"
                        msg += "/stats     — Pattern performance\n"
                        msg += "/summary   — Last 10 days summary\n"
                        msg += "/streak    — Win/loss streak\n"
                        msg += "/best      — Top coins & patterns\n"
                        msg += "/risk      — Risk exposure\n"
                        msg += "/news      — Latest crypto news\n"
                        msg += "/learn     — Bot learning insights\n"
                        msg += "/alerts    — View price alerts\n"
                        msg += "/alert BTC 95000 above — Set price alert\n"
                        msg += "/backtest BTC — Backtest any coin\n"
                        msg += "/help      — This menu"
                        send_telegram(msg)

        except requests.RequestException as e: logger.error(f"Poll request error: {e}")
        except Exception as e:                 logger.error(f"Poll error: {e}",exc_info=True)
        time.sleep(2)

# ================= REPORTS =================
def send_hourly_report():
    r  = f"📊 <b>{BOT_HEADER} Hourly — {get_ist_time()}</b>\n\n"
    r += f"Active: {len(active_trades)} | Pending: {len(pending_signals)}\n"
    r += f"🛡️ Circuit Breaker: {'🔴 ACTIVE' if check_circuit_breaker() else '🟢 OK'}\n\n"
    r += get_pattern_stats_text()
    send_telegram(r)

def send_live_pnl_update():
    if not active_trades: return
    total_pnl=wins=losses=0
    msg=f"⏰ <b>{BOT_HEADER} LIVE PnL — {get_ist_time()}</b>\n\n"
    for coin,t in active_trades.items():
        price=get_price(t["symbol"])
        if not price: continue
        pnl=(((price-t["entry"])/t["entry"])*100*t["leverage"]
             if t["direction"]=="BUY"
             else ((t["entry"]-price)/t["entry"])*100*t["leverage"])
        total_pnl+=pnl
        if pnl>=3: wins+=1
        elif pnl<=-3: losses+=1
        partial=" 💰" if t.get("partial_tp_taken") else ""
        msg+=f"<b>{coin}</b> {t['direction']} | {pnl:+.2f}%{partial}\n"
    total=wins+losses; wr=(wins/total*100) if total>0 else 0
    msg+=f"\n📊 Total: {total_pnl:+.2f}% | ✅{wins} ❌{losses} | 🎯{wr:.1f}%"
    send_telegram(msg)

def send_weekly_report():
    today=datetime.now(IST).date()
    week=[today-timedelta(days=i) for i in range(6,-1,-1)]
    wins=losses=0; total_pnl=0
    msg=f"📅 <b>{BOT_HEADER} Weekly Report — {today.strftime('%d %b %Y')}</b>\n\n"
    for day in week:
        day_t=[j for j in trade_journal if j.get("date")==str(day)]
        w=sum(1 for t in day_t if t["result"]=="WIN")
        l=sum(1 for t in day_t if t["result"]=="LOSS")
        pnl=sum(t["pnl"] for t in day_t)
        wins+=w; losses+=l; total_pnl+=pnl
        msg+=f"{day.strftime('%a %d')}: {w}W/{l}L | {pnl:+.1f}%\n"
    total=wins+losses; wr=(wins/total*100) if total>0 else 0
    msg+=f"\n<b>Week Total:</b> {wins}W/{losses}L | {wr:.1f}% WR | {total_pnl:+.2f}%\n\n"
    msg+=generate_weekly_insight()
    send_telegram(msg)

# ================= RIVER SCAN =================
def scan_river(now: float, market_condition: str):
    global last_river_time
    try:
        if "RIVER" not in active_trades and "RIVER" not in pending_signals:
            price=get_price("RIVERUSDT"); klines=get_klines("RIVERUSDT","15m",100)
            if not price or not klines or len(klines)<50: return
            found=detect_patterns("RIVERUSDT",klines,price,1)+detect_patterns("RIVERUSDT",klines,price,-1)
            seen_set=set(); unique=[]
            for pat in found:
                if (pat[0],pat[2]) not in seen_set:
                    seen_set.add((pat[0],pat[2])); unique.append(pat)
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
                           "leverage":get_smart_leverage("RIVERUSDT",atr_pct,score),
                           "scan_price":price}
                    format_and_send(setup,"RIVER",is_river=True,
                                    is_instant=score>=INSTANT_SIGNAL_THRESHOLD,
                                    market_condition=market_condition)
        last_river_time=now
    except Exception as e: logger.error(f"River error: {e}",exc_info=True)

# ================= COIN SCAN =================
def scan_coins(btc_trend: int, fng: int, market_condition: str):
    for coin in COINS:
        try:
            symbol=coin+"USDT"; price=get_price(symbol); klines=get_klines(symbol,"15m")
            if not price or not klines: continue

            found=detect_patterns(symbol,klines,price,btc_trend)
            if not found: continue

            best=max(found,key=lambda x:x[1])
            if best[1]<MIN_PRIMARY_SCORE:                        continue
            if is_pattern_blacklisted(best[0]):                   continue
            if is_pattern_suspended(best[0]):                     continue
            if not is_sentiment_valid(best[2],fng):               continue
            if coin in BTC_CORRELATED and too_many_correlated_active(): continue

            tf_score=get_timeframe_score(symbol,best[2])
            if tf_score==-1:
                logger.info(f"Skipping {coin} — 4h disagrees"); continue
            if tf_score<2 and best[1]<96:
                logger.info(f"Skipping {coin} — TF score low ({tf_score})"); continue

            confirmed=list(dict.fromkeys([x[0] for x in found]))
            primary=best[0]; extras=[p for p in confirmed if p!=primary]
            pt=primary+(" + "+" + ".join(extras[:2]) if extras else "")
            score=min(best[1]+min(len(found)*0.5,2),99)

            if score>=MIN_SETUP_SCORE:
                atr=calculate_atr(klines); atr_pct=(atr/price)*100 if price>0 else 0
                lev=get_smart_leverage(symbol,atr_pct,score)
                setup={"coin":coin,"symbol":symbol,"direction":best[2],"pattern":pt,
                       "setup_score":score,"leverage":lev,"scan_price":price,
                       "market_condition":market_condition,"tf_score":tf_score}

                if score>=INSTANT_SIGNAL_THRESHOLD:
                    if coin not in active_trades and coin not in pending_signals and len(active_trades)<MAX_ACTIVE_TRADES:
                        logger.info(f"⚡ INSTANT: {coin}|Score:{score}")
                        format_and_send(setup,coin,is_instant=True,market_condition=market_condition)
                else:
                    if (coin not in active_trades and coin not in pending_signals and
                        len(active_trades)<MAX_ACTIVE_TRADES and
                        (coin not in hourly_queue or score>hourly_queue[coin]["setup_score"])):
                        hourly_queue[coin]=setup
                        logger.info(f"Queued: {coin}|{best[2]}|Score:{score}|TF:{tf_score}")

        except Exception as e: logger.error(f"Scan error {coin}: {e}",exc_info=True)
        time.sleep(DELAY_BETWEEN_COINS)

# ================= MAIN LOOP =================
def main():
    global last_batch_time,last_river_time,last_hourly_time,last_pnl_update_time,last_weekly_report_day

    load_active_trades(); load_trade_history(); load_journal()
    load_learning(); load_daily_sent(); load_alerts()
    threading.Thread(target=poll_telegram, daemon=True).start()

    print(STARTUP_ART)
    logger.info(STARTUP_ART)

    send_telegram(
        f"╔══════════════════════════════════╗\n"
        f"║  🚀 TRADING SIGNAL MASTER v32 🚀  ║\n"
        f"║  Smart • Fast • Accurate          ║\n"
        f"╚══════════════════════════════════╝\n\n"
        f"✅ Scanner Running ({len(COINS)} coins)\n"
        f"✅ Queue + River Engine\n"
        f"✅ 4h + 1h Trend Filter\n"
        f"✅ Funding Rate + OI\n"
        f"✅ Whale + Fear & Greed\n"
        f"✅ Circuit Breaker\n"
        f"✅ Instant Signals ≥ {INSTANT_SIGNAL_THRESHOLD}\n"
        f"✅ Smart Position Sizing\n"
        f"✅ Signal Quality Grading\n"
        f"✅ Trailing SL + Partial TP\n"
        f"✅ Learning System + Auto Alerts\n"
        f"✅ Consecutive Loss Suspension\n"
        f"✅ Duplicate Signal Prevention\n"
        f"✅ Backtest Engine\n"
        f"✅ Price Alert System\n"
        f"✅ Weekly AI Insight Report\n"
        f"✅ Setup Score: {MIN_SETUP_SCORE}+\n\n"
        f"📌 Type /help for all commands"
    )
    logger.info(f"TRADING SIGNAL MASTER {BOT_VERSION} started | {len(COINS)} coins | Min score: {MIN_SETUP_SCORE}")

    while True:
        try:
            btc_price=get_price("BTCUSDT"); btc_klines=get_klines("BTCUSDT","1h",100)
            btc_ema50=calculate_ema([float(x[4]) for x in btc_klines],50)

            if not btc_price or btc_ema50 is None:
                logger.warning("BTC data unavailable"); time.sleep(60); continue

            btc_trend=1 if btc_price>btc_ema50 else -1
            fng=get_fear_greed_index()
            market_condition=detect_market_condition(btc_price,btc_klines)

            logger.info(
                f"BTC:{'BULL' if btc_trend==1 else 'BEAR'} | "
                f"Market:{market_condition} | F&G:{fng} | "
                f"Losses:{daily_losses}/{MAX_DAILY_LOSSES}"
            )

            scan_coins(btc_trend,fng,market_condition)
            check_active_trades()
            expire_pending_signals()
            check_price_alerts()

            now=time.time()
            if (now-last_hourly_time)     >=3600:         send_hourly_report();   last_hourly_time=now
            if (now-last_pnl_update_time) >=3600:         send_live_pnl_update(); last_pnl_update_time=now
            if (now-last_batch_time)      >=BATCH_INTERVAL: send_hourly_batch()
            if (now-last_river_time)      >=RIVER_INTERVAL: scan_river(now,market_condition)

            today=datetime.now(IST).date()
            if today.weekday()==6 and last_weekly_report_day!=today:
                send_weekly_report(); last_weekly_report_day=today

            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            logger.error(f"Main loop error: {e}",exc_info=True); time.sleep(60)

if __name__ == "__main__":
    main()
