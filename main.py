import requests
import time
import json
import os
import threading
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot_v32.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8778362544:AAG3Pdr98EySWSpsPLvlM10qUb7TeTPc-u4")
CHAT_ID = os.getenv("CHAT_ID", "8005940008")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

BINANCE_PRICE_URL    = "https://data-api.binance.vision/api/v3/ticker/price"
BINANCE_KLINE_URL    = "https://data-api.binance.vision/api/v3/klines"
BINANCE_AGG_URL      = "https://api.binance.com/api/v3/aggTrades"
BINANCE_FUNDING_URL  = "https://fapi.binance.com/fapi/v1/fundingRate"
BINANCE_OI_URL       = "https://fapi.binance.com/futures/data/openInterestHist"

trade_lock = threading.Lock()
IST = ZoneInfo("Asia/Kolkata")

# ================= COINS LIST (90 COINS) =================
COINS = list(dict.fromkeys([
    # Original core coins
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "AVAX", "SHIB",
    "DOT", "LINK", "BCH", "NEAR", "LTC", "UNI", "APT", "ETC", "HBAR", "FIL",
    "ARB", "VET", "INJ", "OP", "ATOM", "TIA", "SUI", "SEI", "ALGO", "EGLD",
    "FLOW", "EOS", "XTZ", "AAVE", "MKR", "GRT", "SNX", "COMP", "CRV", "SUSHI",
    "LDO", "CAKE", "1INCH", "DYDX", "GMX", "ENS", "PENDLE", "RNDR", "FET", "WLD",
    "AR", "THETA", "LPT", "AKT", "SAND", "MANA", "AXS", "GALA", "CHZ", "APE",
    "GMT", "ENJ", "PEPE", "WIF", "FLOKI", "BONK", "ORDI", "BOME", "NOT", "DOGS",
    # 20 NEW COINS
    "JUP", "PYTH", "JTO", "STRK", "EIGEN", "ETHFI", "IO", "ZERO", "ONDO",
    "BLUR", "CFX", "METIS", "MANTA", "ZETA", "TRB", "ALT", "PIXEL",
    "PORTAL", "STPT", "MANTA"
]))

# ================= STATE MANAGEMENT =================
active_trades    = {}
pending_signals  = {}
hourly_queue     = {}
sent_coins       = []
daily_losses     = 0
last_reset_day   = datetime.now(IST).date()

pattern_stats = {p: {"signals": 0, "wins": 0, "losses": 0, "total_pnl": 0} for p in [
    "EMA Trend", "Breakout", "Pullback to 20 EMA", "RSI Reversal", "Momentum Surge",
    "Volume Spike", "Double Bottom", "Double Top", "Support Bounce", "Resistance Rejection",
    "Bullish Engulfing", "Bearish Engulfing", "Volume Breakout", "Bull Flag Break", "Bear Flag Break"
]}

trade_journal = []  # stores closed trade details for /summary

last_update_id       = None
last_batch_time      = 0
last_river_time      = 0
last_hourly_time     = time.time()
last_pnl_update_time = time.time() + 1800

# ================= CONSTANTS =================
SCAN_INTERVAL            = 300
BATCH_INTERVAL           = 1800
RIVER_INTERVAL           = 900
MIN_SETUP_SCORE          = 94
MIN_PRIMARY_SCORE        = 90
INSTANT_SIGNAL_THRESHOLD = 97       # scores 97+ bypass queue and send immediately
MIN_PROFIT_TARGET        = 20.0
DELAY_BETWEEN_COINS      = 0.15
MAX_SIGNALS_PER_BATCH    = 1
MAX_ACTIVE_TRADES        = 5
SIGNAL_EXPIRY_MINUTES    = 30
INSTANT_EXPIRY_MINUTES   = 15       # instant signals expire faster
ATR_SL_MULTIPLIER        = 2.5
ATR_TP_MULTIPLIER        = 5.0
MAX_DAILY_LOSSES         = 3        # circuit breaker
WHALE_TRADE_THRESHOLD    = 500000   # $500K single trade = whale activity
ATR_VOLATILITY_RATIO     = 3.0      # skip if ATR > 3x slow ATR

BTC_CORRELATED = ["ETH", "BNB", "SOL", "AVAX", "NEAR", "APT", "SUI"]

BOT_VERSION = "v32"
BOT_HEADER  = f"⚙️ BOT {BOT_VERSION}"

# ================= PERSISTENCE =================
def save_active_trades():
    with trade_lock:
        try:
            serializable = {
                k: {
                    **v,
                    "timestamp": v["timestamp"].isoformat(),
                    "expires_at": v["expires_at"].isoformat() if v.get("expires_at") else None
                }
                for k, v in active_trades.items()
            }
            with open("active_trades.json", "w") as f:
                json.dump(serializable, f)
        except Exception as e:
            logger.error(f"Failed to save active trades: {e}")

def load_active_trades():
    global active_trades
    try:
        if os.path.exists("active_trades.json"):
            with open("active_trades.json", "r") as f:
                data = json.load(f)
                active_trades = {
                    k: {
                        **v,
                        "timestamp": datetime.fromisoformat(v["timestamp"]),
                        "expires_at": datetime.fromisoformat(v["expires_at"]) if v.get("expires_at") else None
                    }
                    for k, v in data.items()
                }
            logger.info(f"Loaded {len(active_trades)} active trades.")
    except Exception as e:
        logger.error(f"Failed to load active trades: {e}")

def save_trade_history():
    with trade_lock:
        try:
            with open("trades.json", "w") as f:
                json.dump(pattern_stats, f)
        except Exception as e:
            logger.error(f"Failed to save trade history: {e}")

def load_trade_history():
    global pattern_stats
    try:
        if os.path.exists("trades.json"):
            with open("trades.json", "r") as f:
                loaded = json.load(f)
                for p in pattern_stats.keys():
                    if p in loaded:
                        pattern_stats[p] = loaded[p]
            logger.info("Loaded trade history.")
    except Exception as e:
        logger.error(f"Failed to load trade history: {e}")

def save_journal():
    try:
        with open("journal.json", "w") as f:
            json.dump(trade_journal, f)
    except Exception as e:
        logger.error(f"Failed to save journal: {e}")

def load_journal():
    global trade_journal
    try:
        if os.path.exists("journal.json"):
            with open("journal.json", "r") as f:
                trade_journal = json.load(f)
            logger.info(f"Loaded {len(trade_journal)} journal entries.")
    except Exception as e:
        logger.error(f"Failed to load journal: {e}")

# ================= UTILS =================
def format_price(price: float) -> str:
    if price >= 1000:   return f"{price:.2f}"
    elif price >= 1:    return f"{price:.4f}"
    elif price >= 0.01: return f"{price:.6f}"
    else:               return f"{price:.8f}"

def get_ist_time() -> str:
    return datetime.now(IST).strftime("%I:%M:%S %p IST")

def get_ist_datetime() -> datetime:
    return datetime.now(IST)

# ================= CIRCUIT BREAKER =================
def check_circuit_breaker() -> bool:
    global daily_losses, last_reset_day
    today = datetime.now(IST).date()
    if today != last_reset_day:
        daily_losses = 0
        last_reset_day = today
        logger.info("Circuit breaker reset for new day.")
    return daily_losses >= MAX_DAILY_LOSSES

def increment_daily_losses():
    global daily_losses
    daily_losses += 1
    logger.warning(f"Daily loss count: {daily_losses}/{MAX_DAILY_LOSSES}")
    if daily_losses >= MAX_DAILY_LOSSES:
        send_telegram(
            f"🚨 <b>{BOT_HEADER} CIRCUIT BREAKER ACTIVE</b>\n\n"
            f"3 losses hit today. No more signals until tomorrow.\n"
            f"Protect your capital. 🛡️"
        )

# ================= TELEGRAM HELPER =================
def send_telegram(text: str, parse_mode: str = "HTML", reply_markup: dict = None) -> bool:
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=payload, timeout=15
        )
        if res.status_code != 200:
            logger.warning(f"Telegram send failed [{res.status_code}]: {res.text}")
        return res.status_code == 200
    except requests.RequestException as e:
        logger.error(f"Telegram request error: {e}")
        return False

# ================= BINANCE HELPERS =================
def get_price(symbol: str) -> float | None:
    try:
        res = requests.get(BINANCE_PRICE_URL, params={"symbol": symbol}, timeout=10)
        if res.status_code == 200:
            return float(res.json()["price"])
        logger.warning(f"Non-200 price response {symbol}: {res.status_code}")
        return None
    except requests.RequestException as e:
        logger.warning(f"Price fetch failed {symbol}: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"Price parse error {symbol}: {e}")
        return None

def get_klines(symbol: str, interval: str, limit: int = 100) -> list:
    try:
        res = requests.get(
            BINANCE_KLINE_URL,
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10
        )
        if res.status_code == 200:
            return res.json()
        logger.warning(f"Non-200 klines response {symbol}: {res.status_code}")
        return []
    except requests.RequestException as e:
        logger.warning(f"Klines fetch failed {symbol}: {e}")
        return []

# ================= INDICATORS =================
def calculate_ema(closes: list, period: int) -> float | None:
    if len(closes) < period:
        return None
    ema = sum(closes[:period]) / period
    k = 2 / (period + 1)
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def calculate_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(0, diff))
        losses.append(max(0, -diff))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    return 100 - (100 / (1 + (avg_gain / avg_loss))) if avg_loss != 0 else 100

def calculate_atr(klines: list, period: int = 14) -> float:
    if len(klines) < period + 1:
        return 0
    trs = []
    for i in range(1, len(klines)):
        high       = float(klines[i][2])
        low        = float(klines[i][3])
        prev_close = float(klines[i - 1][4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-period:]) / period

# ================= FUNDING RATE =================
def get_funding_rate(symbol: str) -> float | None:
    try:
        res = requests.get(
            BINANCE_FUNDING_URL,
            params={"symbol": symbol, "limit": 1},
            timeout=10
        )
        if res.status_code == 200:
            data = res.json()
            if data:
                return float(data[0]["fundingRate"])
        return None
    except requests.RequestException as e:
        logger.warning(f"Funding rate fetch failed {symbol}: {e}")
        return None
    except (KeyError, ValueError, IndexError) as e:
        logger.warning(f"Funding rate parse error {symbol}: {e}")
        return None

def is_funding_favorable(symbol: str, direction: str) -> bool:
    rate = get_funding_rate(symbol)
    if rate is None:
        return True  # don't block if data unavailable
    if direction == "BUY"  and rate >  0.001:
        logger.info(f"{symbol} funding rate {rate:.4f} — longs overcrowded, skipping BUY")
        return False
    if direction == "SELL" and rate < -0.001:
        logger.info(f"{symbol} funding rate {rate:.4f} — shorts overcrowded, skipping SELL")
        return False
    return True

# ================= OPEN INTEREST =================
def get_oi_trend(symbol: str) -> bool | None:
    try:
        res = requests.get(
            BINANCE_OI_URL,
            params={"symbol": symbol, "period": "15m", "limit": 5},
            timeout=10
        )
        if res.status_code == 200:
            data = res.json()
            if len(data) >= 2:
                oi_now  = float(data[-1]["sumOpenInterest"])
                oi_prev = float(data[-2]["sumOpenInterest"])
                return oi_now > oi_prev
        return None
    except requests.RequestException as e:
        logger.warning(f"OI fetch failed {symbol}: {e}")
        return None
    except (KeyError, ValueError, IndexError) as e:
        logger.warning(f"OI parse error {symbol}: {e}")
        return None

# ================= WHALE DETECTION =================
def has_whale_activity(symbol: str) -> bool:
    try:
        res = requests.get(
            BINANCE_AGG_URL,
            params={"symbol": symbol, "limit": 20},
            timeout=10
        )
        if res.status_code == 200:
            for t in res.json():
                trade_value = float(t["p"]) * float(t["q"])
                if trade_value > WHALE_TRADE_THRESHOLD:
                    logger.info(f"Whale activity detected on {symbol}: ${trade_value:,.0f}")
                    return True
        return False
    except requests.RequestException as e:
        logger.warning(f"Whale check failed {symbol}: {e}")
        return False
    except (KeyError, ValueError) as e:
        logger.warning(f"Whale parse error {symbol}: {e}")
        return False

# ================= FEAR & GREED INDEX =================
def get_fear_greed_index() -> int:
    try:
        res = requests.get(
            "https://api.alternative.me/fng/?limit=1",
            timeout=10
        )
        if res.status_code == 200:
            value = int(res.json()["data"][0]["value"])
            logger.info(f"Fear & Greed Index: {value}")
            return value
        return 50
    except requests.RequestException as e:
        logger.warning(f"Fear & Greed fetch failed: {e}")
        return 50
    except (KeyError, ValueError, IndexError) as e:
        logger.warning(f"Fear & Greed parse error: {e}")
        return 50

def is_sentiment_valid(direction: str, fng: int) -> bool:
    if direction == "BUY"  and fng < 20:
        logger.info(f"Extreme fear ({fng}) — skipping BUY signal")
        return False
    if direction == "SELL" and fng > 80:
        logger.info(f"Extreme greed ({fng}) — skipping SELL signal")
        return False
    return True

# ================= HIGHER TIMEFRAME TREND =================
def get_htf_trend(symbol: str, interval: str = "1h") -> int:
    try:
        klines = get_klines(symbol, interval, 50)
        if not klines or len(klines) < 50:
            return 0
        closes = [float(k[4]) for k in klines]
        ema20  = calculate_ema(closes, 20)
        ema50  = calculate_ema(closes, 50)
        if ema20 and ema50:
            return 1 if ema20 > ema50 else -1
        return 0
    except Exception as e:
        logger.warning(f"HTF trend check failed {symbol} {interval}: {e}")
        return 0

def get_timeframe_score(symbol: str, direction: str) -> int:
    direction_int = 1 if direction == "BUY" else -1
    htf_4h = get_htf_trend(symbol, "4h")
    htf_1h = get_htf_trend(symbol, "1h")

    # 4h is mandatory — hard block if it disagrees
    if htf_4h != 0 and htf_4h != direction_int:
        return -1

    score = 0
    if htf_4h == direction_int:
        score += 2
    if htf_1h == direction_int:
        score += 1
    return score  # 3=perfect, 2=good, 1=weak, -1=blocked

# ================= STRUCTURE SL =================
def get_structure_sl(klines: list, direction: str, entry: float, atr: float) -> float:
    lows  = [float(k[3]) for k in klines[-20:]]
    highs = [float(k[2]) for k in klines[-20:]]
    if direction == "BUY":
        structure_sl = min(lows) * 0.998
        atr_sl       = entry - (atr * ATR_SL_MULTIPLIER)
        return min(structure_sl, atr_sl)
    else:
        structure_sl = max(highs) * 1.002
        atr_sl       = entry + (atr * ATR_SL_MULTIPLIER)
        return max(structure_sl, atr_sl)

# ================= FILTERS =================
def is_volume_confirmed(klines: list) -> bool:
    vols = [float(k[5]) for k in klines]
    if len(vols) < 20:
        return False
    avg_vol = sum(vols[-20:]) / 20
    return vols[-1] > avg_vol * 1.2

def is_rsi_valid(closes: list, direction: str) -> bool:
    rsi = calculate_rsi(closes)
    if direction == "BUY"  and rsi > 72:
        return False
    if direction == "SELL" and rsi < 28:
        return False
    return True

def is_volatility_normal(klines: list) -> bool:
    atr_now  = calculate_atr(klines, 14)
    atr_slow = calculate_atr(klines, 50)
    if atr_slow == 0:
        return True
    if (atr_now / atr_slow) > ATR_VOLATILITY_RATIO:
        logger.info("Abnormal volatility detected — skipping")
        return False
    return True

def is_pattern_blacklisted(pattern_name: str) -> bool:
    stats = pattern_stats.get(pattern_name)
    if not stats or stats["signals"] < 10:
        return False
    win_rate = (stats["wins"] / stats["signals"]) * 100
    if win_rate < 40:
        logger.info(f"Pattern blacklisted ({win_rate:.1f}% win rate): {pattern_name}")
        return True
    return False

def too_many_correlated_active() -> bool:
    return sum(1 for coin in active_trades if coin in BTC_CORRELATED) >= 2

# ================= SIGNAL EXPIRY =================
def expire_pending_signals():
    now     = get_ist_datetime()
    expired = [
        coin for coin, sig in list(pending_signals.items())
        if sig.get("expires_at") and now > sig["expires_at"]
    ]
    for coin in expired:
        del pending_signals[coin]
        send_telegram(f"⏰ <b>{BOT_HEADER}</b> Signal expired: <b>{coin}</b> — not activated in time.")
        logger.info(f"Signal expired: {coin}")

# ================= MISC HELPERS =================
def get_news_headlines(coin: str) -> list:
    if not NEWS_API_KEY:
        return []
    try:
        res = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={"auth_token": NEWS_API_KEY, "currencies": coin, "kind": "news"},
            timeout=5
        )
        return [p["title"] for p in res.json().get("results", [])[:3]]
    except requests.RequestException as e:
        logger.warning(f"News fetch failed {coin}: {e}")
        return []
    except (KeyError, ValueError) as e:
        logger.warning(f"News parse error {coin}: {e}")
        return []

def get_dynamic_leverage(symbol: str, atr_pct: float, confidence: float) -> int:
    base = symbol.replace("USDT", "")
    if base in ["BTC", "ETH"]:              return 10
    if base in ["BNB", "SOL"]:              return 8
    if atr_pct < 2.0 and confidence > 80:   return 8
    if atr_pct < 4.0:                       return 5
    return 4

def get_active_trades_text() -> str:
    if not active_trades:
        return f"📊 <b>{BOT_HEADER}</b>\nNo active trades."
    text = f"📊 <b>{BOT_HEADER} Active Trades ({len(active_trades)})</b>\n\n"
    for coin, trade in active_trades.items():
        text += f"<b>{coin}</b> {trade['direction']}\n"
        text += f"Entry: {format_price(trade['entry'])} | SL: {format_price(trade['sl'])}\n"
        text += f"TP: {format_price(trade['tp'])} | Lev: {trade['leverage']}x\n\n"
    return text

def get_pattern_stats_text() -> str:
    text = f"📈 <b>{BOT_HEADER} Pattern Performance</b>\n\n"
    sorted_patterns = sorted(pattern_stats.items(), key=lambda x: x[1]["signals"], reverse=True)
    for pattern, stats in sorted_patterns[:10]:
        if stats["signals"] > 0:
            win_rate = (stats["wins"] / stats["signals"]) * 100
            text += f"<b>{pattern}</b>\n"
            text += f"Signals: {stats['signals']} | Win: {win_rate:.1f}% | PnL: {stats['total_pnl']:.1f}%\n\n"
    return text

def get_summary_text() -> str:
    today     = datetime.now(IST).date()
    today_str = today.strftime("%d %b %Y")

    today_trades = [j for j in trade_journal if j.get("date") == str(today)]
    wins   = sum(1 for t in today_trades if t["result"] == "WIN")
    losses = sum(1 for t in today_trades if t["result"] == "LOSS")
    total  = wins + losses
    total_pnl  = sum(t["pnl"] for t in today_trades)
    win_rate   = (wins / total * 100) if total > 0 else 0

    best_pattern = None
    if today_trades:
        from collections import Counter
        win_patterns = [t["pattern"] for t in today_trades if t["result"] == "WIN"]
        if win_patterns:
            best_pattern = Counter(win_patterns).most_common(1)[0][0]

    cb_status = "🔴 ACTIVE" if check_circuit_breaker() else "🟢 OK"

    text  = f"📋 <b>{BOT_HEADER} Daily Summary — {today_str}</b>\n\n"
    text += f"✅ Wins: {wins} | ❌ Losses: {losses}\n"
    text += f"🎯 Win Rate: {win_rate:.1f}%\n"
    text += f"💰 Total PnL: {total_pnl:+.2f}%\n"
    text += f"📌 Active Trades: {len(active_trades)}\n"
    text += f"⏳ Pending Signals: {len(pending_signals)}\n"
    text += f"🛡️ Circuit Breaker: {cb_status} ({daily_losses}/{MAX_DAILY_LOSSES})\n"
    if best_pattern:
        text += f"⭐ Best Pattern Today: {best_pattern}\n"
    return text

# ================= PATTERN DETECTION =================
def detect_patterns(symbol: str, klines: list, price: float, btc_trend: int) -> list:
    if len(klines) < 50:
        return []
    closes  = [float(k[4]) for k in klines]
    opens   = [float(k[1]) for k in klines]
    highs   = [float(k[2]) for k in klines]
    lows    = [float(k[3]) for k in klines]
    vols    = [float(k[5]) for k in klines]
    avg_vol = sum(vols[-20:]) / 20
    rsi     = calculate_rsi(closes)
    ema20   = calculate_ema(closes, 20)
    ema50   = calculate_ema(closes, 50)

    market_range = ((max(highs[-20:]) - min(lows[-20:])) / price) * 100
    if market_range < 1.8:
        return []

    patterns = []

    if ema20 and closes[-1] > highs[-2] and closes[-2] > highs[-3] and price > ema20 and btc_trend == 1:
        patterns.append(("Bull Flag Break", 94, "BUY"))

    if ema20 and closes[-1] < lows[-2] and closes[-2] < lows[-3] and price < ema20 and btc_trend == -1:
        patterns.append(("Bear Flag Break", 94, "SELL"))

    if closes[-1] > max(highs[-20:-1]) and vols[-1] > avg_vol * 1.5:
        if btc_trend == 1:  patterns.append(("Breakout", 88, "BUY"))
    elif closes[-1] < min(lows[-20:-1]) and vols[-1] > avg_vol * 1.5:
        if btc_trend == -1: patterns.append(("Breakout", 88, "SELL"))

    if opens[-2] > closes[-2] and opens[-1] < closes[-2] and closes[-1] > opens[-2]:
        if btc_trend == 1:  patterns.append(("Bullish Engulfing", 90, "BUY"))
    elif opens[-2] < closes[-2] and opens[-1] > closes[-2] and closes[-1] < opens[-2]:
        if btc_trend == -1: patterns.append(("Bearish Engulfing", 90, "SELL"))

    if ema20 and ema50:
        if price > ema20 > ema50 and btc_trend == 1:
            patterns.append(("EMA Trend", 85, "BUY"))
        elif price < ema20 < ema50 and btc_trend == -1:
            patterns.append(("EMA Trend", 85, "SELL"))

    if ema20 and abs(price - ema20) / ema20 < 0.005:
        patterns.append(("Pullback to 20 EMA", 82, "BUY" if price > ema20 else "SELL"))

    if rsi < 30:   patterns.append(("RSI Reversal", 80, "BUY"))
    elif rsi > 70: patterns.append(("RSI Reversal", 80, "SELL"))

    mom = (closes[-1] - closes[-3]) / closes[-3] * 100 if len(closes) > 3 else 0
    if mom > 3 and btc_trend == 1:
        patterns.append(("Momentum Surge", 87, "BUY"))
    elif mom < -3 and btc_trend == -1:
        patterns.append(("Momentum Surge", 87, "SELL"))

    if vols[-1] > avg_vol * 3.5:
        patterns.append(("Volume Spike", 84, "BUY" if closes[-1] > opens[-1] else "SELL"))

    support    = min(lows[-30:-1])
    resistance = max(highs[-30:-1])

    if price <= support * 1.005 and closes[-1] > opens[-1]:
        patterns.append(("Support Bounce", 88, "BUY"))
    if price >= resistance * 0.995 and closes[-1] < opens[-1]:
        patterns.append(("Resistance Rejection", 88, "SELL"))

    if len(lows) > 40:
        if abs(min(lows[-40:-20]) - min(lows[-10:])) / price < 0.005:
            patterns.append(("Double Bottom", 90, "BUY"))
        if abs(max(highs[-40:-20]) - max(highs[-10:])) / price < 0.005:
            patterns.append(("Double Top", 90, "SELL"))

    if price > resistance and vols[-1] > avg_vol * 2.5 and btc_trend == 1:
        patterns.append(("Volume Breakout", 91, "BUY"))

    return patterns

# ================= TRAILING STOP LOSS =================
def update_trailing_sl(coin: str, trade: dict, price: float):
    trail_distance = abs(trade["tp"] - trade["entry"]) * 0.3
    if trade["direction"] == "BUY":
        new_sl = price - trail_distance
        if new_sl > trade["sl"]:
            active_trades[coin]["sl"] = new_sl
            save_active_trades()
            logger.info(f"Trailing SL updated {coin}: {format_price(new_sl)}")
    else:
        new_sl = price + trail_distance
        if new_sl < trade["sl"]:
            active_trades[coin]["sl"] = new_sl
            save_active_trades()
            logger.info(f"Trailing SL updated {coin}: {format_price(new_sl)}")

# ================= PARTIAL TAKE PROFIT =================
def check_partial_tp(coin: str, trade: dict, price: float, current_pnl: float):
    if trade.get("partial_tp_taken"):
        return
    halfway_pnl = ((abs(trade["tp"] - trade["entry"]) / 2) / trade["entry"]) * 100 * trade["leverage"]
    if current_pnl >= halfway_pnl:
        active_trades[coin]["partial_tp_taken"] = True
        active_trades[coin]["sl"] = trade["entry"]  # move SL to breakeven
        save_active_trades()
        send_telegram(
            f"💰 <b>{BOT_HEADER} PARTIAL TP HIT: {coin}</b>\n\n"
            f"50% position closed at {format_price(price)}\n"
            f"SL moved to entry — rest runs free 🎯\n"
            f"PnL so far: {current_pnl:+.2f}%"
        )
        logger.info(f"Partial TP taken: {coin} at {format_price(price)}")

# ================= VERIFICATION & SENDING =================
def format_and_send(setup: dict, coin: str, is_river: bool = False, is_instant: bool = False) -> bool:
    global pending_signals, sent_coins

    # Circuit breaker check
    if check_circuit_breaker():
        logger.info(f"Circuit breaker active — blocking signal for {coin}")
        return False

    # Live price
    live_price = get_price(setup["symbol"])
    if not live_price:
        logger.warning(f"Could not get live price for {setup['symbol']}")
        return False

    entry = live_price

    # Price drift check
    price_diff = abs(entry - setup["scan_price"]) / setup["scan_price"]
    if price_diff > 0.005:
        logger.info(f"{coin} rejected — price drifted {price_diff:.2%}")
        return False

    # Klines
    klines = get_klines(setup["symbol"], "15m")
    if not klines:
        logger.warning(f"Could not get klines for {setup['symbol']}")
        return False

    closes  = [float(x[4]) for x in klines]
    atr     = calculate_atr(klines)
    atr_pct = (atr / entry) * 100 if entry > 0 else 0

    # All filters
    if not is_volume_confirmed(klines):
        logger.info(f"{coin} rejected — volume not confirmed")
        return False

    if not is_rsi_valid(closes, setup["direction"]):
        logger.info(f"{coin} rejected — RSI unfavorable")
        return False

    if not is_volatility_normal(klines):
        logger.info(f"{coin} rejected — abnormal volatility")
        return False

    if not is_funding_favorable(setup["symbol"], setup["direction"]):
        logger.info(f"{coin} rejected — funding rate unfavorable")
        return False

    oi_rising = get_oi_trend(setup["symbol"])
    if oi_rising is False:
        logger.info(f"{coin} rejected — OI falling, weak momentum")
        return False

    # Leverage
    lev = setup.get("leverage", get_dynamic_leverage(setup["symbol"], atr_pct, setup["setup_score"]))

    # Structure SL + ATR TP
    sl = get_structure_sl(klines, setup["direction"], entry, atr)
    tp = entry + (atr * ATR_TP_MULTIPLIER) if setup["direction"] == "BUY" else entry - (atr * ATR_TP_MULTIPLIER)

    # Profit target check
    profit_target = (abs(tp - entry) / entry) * 100 * lev
    if profit_target < MIN_PROFIT_TARGET:
        risk_per_unit = abs(tp - entry) / entry
        if risk_per_unit > 0:
            needed_lev = int(MIN_PROFIT_TARGET / (risk_per_unit * 100)) + 1
            if needed_lev <= 10:
                lev = needed_lev
                profit_target = (abs(tp - entry) / entry) * 100 * lev
            else:
                logger.info(f"{coin} rejected — profit target too low")
                return False

    setup["leverage"] = lev

    # ETA and expiry
    price_range    = (max(closes[-10:]) - min(closes[-10:])) / 10
    eta            = int(abs(tp - entry) / (price_range if price_range > 0 else 0.001) * 15)
    expiry_minutes = INSTANT_EXPIRY_MINUTES if is_instant else SIGNAL_EXPIRY_MINUTES
    expiry_time    = get_ist_datetime() + timedelta(minutes=expiry_minutes)
    expiry_str     = expiry_time.strftime("%I:%M %p IST")

    mom     = (closes[-1] - closes[-3]) / closes[-3] * 100
    rsi_val = calculate_rsi(closes)
    news    = get_news_headlines(coin)
    whale   = has_whale_activity(setup["symbol"])

    # Timeframe score label
    tf_score = setup.get("tf_score", 0)
    tf_label = "4h✅ 1h✅" if tf_score == 3 else "4h✅ 1h⚠️" if tf_score == 2 else "4h✅"

    # Build message header
    if is_instant:
        header = f"⚡ <b>{BOT_HEADER} INSTANT SIGNAL {coin}</b>"
    elif is_river:
        header = f"🌊 <b>{BOT_HEADER} RIVER SIGNAL</b>"
    else:
        header = f"🔥 <b>{BOT_HEADER} VERIFIED SETUP {coin}</b>"

    msg  = f"{header} | Score: {int(setup['setup_score'])}/100\n\n"
    msg += f"📢 Direction: {setup['direction']} | Leverage: {lev}x\n"
    msg += f"💰 Entry: {format_price(entry)}\n"
    msg += f"🎯 TP: {format_price(tp)}\n"
    msg += f"🛑 SL: {format_price(sl)}\n\n"
    msg += f"📈 Profit Target: {profit_target:.2f}%\n"
    msg += f"📌 Pattern: {setup['pattern']} | RSI: {rsi_val:.2f}\n"
    msg += f"⚡ Momentum: {mom:.2f}% | 🚀 Velocity: {abs(mom / 45):.4f}/min\n"
    msg += f"📊 Timeframes: {tf_label}\n"
    msg += f"🐋 Whale Activity: {'✅ YES' if whale else '❌ No'}\n"
    msg += f"⏳ ETA: ~{eta} mins | ⏰ Expires: {expiry_str}\n"
    msg += f"✏️ ATR: {format_price(atr)}\n"
    if is_instant:
        msg += f"\n⚡ <b>INSTANT SIGNAL — Act within {expiry_minutes} mins!</b>\n"
    if news:
        msg += "\n<b>📰 News:</b>\n" + "\n".join([f"• {n[:60]}..." for n in news]) + "\n"
    msg += f"\n⏰ Sent At: {get_ist_time()}"

    setup.update({
        "entry": entry, "sl": sl, "tp": tp,
        "timestamp": get_ist_datetime(),
        "expires_at": expiry_time,
        "reversal_alerted": False,
        "breakeven_sent": False,
        "partial_tp_taken": False
    })
    pending_signals[coin] = setup

    reply_markup = {
        "inline_keyboard": [[
            {"text": "✅ Activate Trade", "callback_data": f"ACTIVATE_{coin}"},
            {"text": "❌ Ignore",          "callback_data": f"IGNORE_{coin}"}
        ]]
    }

    if send_telegram(msg, reply_markup=reply_markup):
        sent_coins.append(setup["coin"])
        logger.info(f"Signal sent: {coin} | {setup['direction']} | Score: {setup['setup_score']} | Instant: {is_instant}")
        return True
    return False

# ================= BATCH SENDING =================
def send_hourly_batch():
    global hourly_queue, last_batch_time, sent_coins
    if not hourly_queue:
        return
    sorted_queue = sorted(hourly_queue.values(), key=lambda x: x["setup_score"], reverse=True)
    sent_count = 0
    for setup in sorted_queue:
        if setup["coin"] == "RIVER":
            continue
        if sent_count >= MAX_SIGNALS_PER_BATCH:
            break
        if format_and_send(setup, setup["coin"]):
            sent_count += 1
    for setup in sorted_queue:
        if setup["coin"] in hourly_queue:
            del hourly_queue[setup["coin"]]
    sent_coins      = []
    last_batch_time = time.time()

# ================= ACTIVE TRADE MONITORING =================
def check_active_trades():
    for coin, trade in list(active_trades.items()):
        price = get_price(trade["symbol"])
        if not price:
            continue

        # PnL
        if trade["direction"] == "BUY":
            current_pnl = ((price - trade["entry"]) / trade["entry"]) * 100 * trade["leverage"]
        else:
            current_pnl = ((trade["entry"] - price) / trade["entry"]) * 100 * trade["leverage"]

        # Trailing SL
        update_trailing_sl(coin, trade, price)

        # Partial TP
        check_partial_tp(coin, trade, price, current_pnl)

        # Reversal alert
        if not trade.get("reversal_alerted", False):
            klines = get_klines(trade["symbol"], "15m", 20)
            if klines:
                closes = [float(x[4]) for x in klines]
                ema20  = calculate_ema(closes, 20)
                if ema20:
                    reversal = (
                        (trade["direction"] == "BUY"  and price < ema20 * 0.995) or
                        (trade["direction"] == "SELL" and price > ema20 * 1.005)
                    )
                    if reversal:
                        send_telegram(f"⚠️ <b>{BOT_HEADER} TREND REVERSAL {coin}</b>\nPrice broke EMA20. Consider exiting.")
                        active_trades[coin]["reversal_alerted"] = True
                        save_active_trades()

        # Breakeven alert
        if not trade.get("breakeven_sent", False) and current_pnl >= 10:
            send_telegram(
                f"🟡 <b>{BOT_HEADER} BREAK-EVEN ALERT {coin}</b>\n\n"
                f"Trade reached +10% profit.\nConsider moving SL to entry.\n"
                f"Current PnL: {current_pnl:.2f}%"
            )
            active_trades[coin]["breakeven_sent"] = True
            save_active_trades()

        # TP / SL check
        hit = None
        if trade["direction"] == "BUY":
            if price >= trade["tp"]:   hit = "WIN"
            elif price <= trade["sl"]: hit = "LOSS"
        else:
            if price <= trade["tp"]:   hit = "WIN"
            elif price >= trade["sl"]: hit = "LOSS"

        if hit:
            with trade_lock:
                primary_pattern = trade["pattern"].split(" + ")[0]
                pnl_result      = current_pnl

                if primary_pattern in pattern_stats:
                    pattern_stats[primary_pattern]["signals"]   += 1
                    pattern_stats[primary_pattern]["total_pnl"] += pnl_result
                    if hit == "WIN":
                        pattern_stats[primary_pattern]["wins"]   += 1
                    else:
                        pattern_stats[primary_pattern]["losses"] += 1
                        increment_daily_losses()

                # Trade journal entry
                duration = ""
                if trade.get("timestamp"):
                    mins     = int((get_ist_datetime() - trade["timestamp"]).total_seconds() / 60)
                    duration = f"{mins} mins"

                trade_journal.append({
                    "date":      str(datetime.now(IST).date()),
                    "coin":      coin,
                    "direction": trade["direction"],
                    "pattern":   primary_pattern,
                    "entry":     trade["entry"],
                    "exit":      price,
                    "pnl":       pnl_result,
                    "result":    hit,
                    "duration":  duration,
                    "htf_score": trade.get("tf_score", 0)
                })
                save_journal()

            send_telegram(
                f"{'✅' if hit == 'WIN' else '🛑'} <b>{BOT_HEADER} Trade Closed: {coin}</b> ({hit})\n\n"
                f"Entry: {format_price(trade['entry'])} → Exit: {format_price(price)}\n"
                f"Pattern: {primary_pattern}\n"
                f"Duration: {duration}\n"
                f"PnL: {pnl_result:+.2f}%"
            )
            del active_trades[coin]
            save_active_trades()
            save_trade_history()
            logger.info(f"Trade closed: {coin} | {hit} | PnL: {pnl_result:.2f}%")

# ================= TELEGRAM POLLING =================
def poll_telegram():
    global last_update_id
    while True:
        try:
            params = {}
            if last_update_id is not None:
                params["offset"] = last_update_id + 1

            res = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params=params, timeout=15
            )
            if res.status_code != 200:
                logger.warning(f"Telegram getUpdates failed: {res.status_code}")
                time.sleep(2)
                continue

            for update in res.json().get("result", []):
                last_update_id = update["update_id"]

                if "callback_query" in update:
                    cb   = update["callback_query"]
                    data = cb["data"]
                    coin = data.split("_")[1]

                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                            json={"callback_query_id": cb["id"], "text": "Processing..."},
                            timeout=15
                        )
                    except requests.RequestException as e:
                        logger.warning(f"answerCallbackQuery failed: {e}")

                    if "ACTIVATE" in data and coin in pending_signals:
                        live_price = get_price(pending_signals[coin]["symbol"])
                        if live_price:
                            pending_signals[coin]["entry"] = live_price
                        with trade_lock:
                            pending_signals[coin]["breakeven_sent"]  = False
                            pending_signals[coin]["partial_tp_taken"] = False
                            active_trades[coin] = pending_signals[coin]
                        save_active_trades()
                        send_telegram(
                            f"🚀 <b>{BOT_HEADER} {coin} Activated!</b>\n"
                            f"Entry recorded at {format_price(pending_signals[coin]['entry'])}"
                        )
                        del pending_signals[coin]
                        logger.info(f"Trade activated: {coin}")

                    elif "IGNORE" in data and coin in pending_signals:
                        send_telegram(f"❌ <b>{BOT_HEADER}</b> {coin} Ignored")
                        del pending_signals[coin]
                        logger.info(f"Signal ignored: {coin}")

                elif "message" in update:
                    text = update["message"].get("text", "").lower()
                    if text == "/stats":
                        send_telegram(get_pattern_stats_text())
                    elif text == "/trades":
                        send_telegram(get_active_trades_text())
                    elif text == "/summary":
                        send_telegram(get_summary_text())
                    elif text == "/pending":
                        if pending_signals:
                            msg = f"⏳ <b>{BOT_HEADER} Pending Signals ({len(pending_signals)})</b>\n\n"
                            for c, sig in pending_signals.items():
                                exp     = sig.get("expires_at")
                                exp_str = exp.strftime("%I:%M %p IST") if exp else "N/A"
                                msg    += f"<b>{c}</b> {sig['direction']} | Score: {sig['setup_score']} | Expires: {exp_str}\n"
                            send_telegram(msg)
                        else:
                            send_telegram(f"<b>{BOT_HEADER}</b> No pending signals.")
                    elif text == "/help":
                        help_msg  = f"📖 <b>{BOT_HEADER} Commands</b>\n\n"
                        help_msg += "/trades — Active trades\n"
                        help_msg += "/pending — Pending signals\n"
                        help_msg += "/stats — Pattern performance\n"
                        help_msg += "/summary — Today's summary\n"
                        help_msg += "/help — This menu"
                        send_telegram(help_msg)

        except requests.RequestException as e:
            logger.error(f"Telegram polling request error: {e}")
        except Exception as e:
            logger.error(f"Telegram polling unexpected error: {e}", exc_info=True)

        time.sleep(2)

# ================= REPORTS =================
def send_hourly_report():
    report  = f"📊 <b>{BOT_HEADER} Hourly Report — {get_ist_time()}</b>\n\n"
    report += f"Active: {len(active_trades)} | Pending: {len(pending_signals)}\n"
    report += f"🛡️ Circuit Breaker: {'🔴 ACTIVE' if check_circuit_breaker() else '🟢 OK'}\n\n"
    report += get_pattern_stats_text()
    send_telegram(report)

def send_live_pnl_update():
    if not active_trades:
        return
    total_pnl = 0
    wins = 0
    losses = 0
    msg = f"⏰ <b>{BOT_HEADER} LIVE PnL UPDATE</b> — {get_ist_time()}\n\n"
    for coin, trade in active_trades.items():
        price = get_price(trade["symbol"])
        if not price:
            continue
        pnl = (
            ((price - trade["entry"]) / trade["entry"]) * 100 * trade["leverage"]
            if trade["direction"] == "BUY"
            else ((trade["entry"] - price) / trade["entry"]) * 100 * trade["leverage"]
        )
        total_pnl += pnl
        if pnl >= 3:   wins   += 1
        elif pnl <= -3: losses += 1
        partial = "💰 Partial TP taken" if trade.get("partial_tp_taken") else ""
        msg += f"<b>{coin}</b> {trade['direction']}\nPnL: {pnl:+.2f}% {partial}\n\n"
    total    = wins + losses
    win_rate = (wins / total * 100) if total > 0 else 0
    msg += f"📊 Total PnL: {total_pnl:+.2f}%\n"
    msg += f"✅ Winning: {wins} | ❌ Losing: {losses}\n"
    msg += f"🎯 Win Rate: {win_rate:.1f}%\n"
    msg += f"📌 Active: {len(active_trades)}"
    send_telegram(msg)

# ================= RIVER SCAN =================
def scan_river(now: float):
    global last_river_time
    try:
        if "RIVER" not in active_trades and "RIVER" not in pending_signals:
            price  = get_price("RIVERUSDT")
            klines = get_klines("RIVERUSDT", "15m", 100)
            if not price or not klines or len(klines) < 50:
                return

            found = (
                detect_patterns("RIVERUSDT", klines, price, 1) +
                detect_patterns("RIVERUSDT", klines, price, -1)
            )
            seen, unique_patterns = set(), []
            for pat in found:
                key = (pat[0], pat[2])
                if key not in seen:
                    seen.add(key)
                    unique_patterns.append(pat)

            if unique_patterns:
                best = max(unique_patterns, key=lambda x: x[1])
                if best[1] < MIN_PRIMARY_SCORE:
                    return

                confirmed    = list(dict.fromkeys([x[0] for x in unique_patterns]))
                primary      = best[0]
                extras       = [p for p in confirmed if p != primary]
                pattern_text = primary + (" + " + " + ".join(extras[:2]) if extras else "")
                bonus        = min(len(unique_patterns) * 0.5, 2)
                score        = min(best[1] + bonus, 99)

                if score >= 82:
                    atr     = calculate_atr(klines)
                    atr_pct = (atr / price) * 100 if price > 0 else 0
                    lev     = get_dynamic_leverage("RIVERUSDT", atr_pct, score)
                    river_setup = {
                        "coin": "RIVER", "symbol": "RIVERUSDT",
                        "direction": best[2], "pattern": pattern_text,
                        "setup_score": score, "leverage": lev, "scan_price": price
                    }
                    is_instant = score >= INSTANT_SIGNAL_THRESHOLD
                    format_and_send(river_setup, "RIVER", is_river=True, is_instant=is_instant)
        last_river_time = now
    except Exception as e:
        logger.error(f"River scan error: {e}", exc_info=True)

# ================= COIN SCAN =================
def scan_coins(btc_trend: int, fng: int):
    for coin in COINS:
        try:
            symbol = coin + "USDT"
            price  = get_price(symbol)
            klines = get_klines(symbol, "15m")
            if not price or not klines:
                continue

            found = detect_patterns(symbol, klines, price, btc_trend)
            if not found:
                continue

            best = max(found, key=lambda x: x[1])

            # Primary pattern gate
            if best[1] < MIN_PRIMARY_SCORE:
                continue

            # Pattern blacklist check
            if is_pattern_blacklisted(best[0]):
                continue

            # Sentiment filter
            if not is_sentiment_valid(best[2], fng):
                continue

            # Correlation filter
            if coin in BTC_CORRELATED and too_many_correlated_active():
                logger.info(f"Skipping {coin} — too many correlated trades active")
                continue

            # Tiered timeframe alignment
            tf_score = get_timeframe_score(symbol, best[2])
            if tf_score == -1:
                logger.info(f"Skipping {coin} — 4h disagrees with signal direction")
                continue
            if tf_score < 2 and best[1] < 96:
                logger.info(f"Skipping {coin} — timeframe score too low ({tf_score})")
                continue

            confirmed    = list(dict.fromkeys([x[0] for x in found]))
            primary      = best[0]
            extras       = [p for p in confirmed if p != primary]
            pattern_text = primary + (" + " + " + ".join(extras[:2]) if extras else "")
            bonus        = min(len(found) * 0.5, 2)
            score        = min(best[1] + bonus, 99)

            if score >= MIN_SETUP_SCORE:
                atr     = calculate_atr(klines)
                atr_pct = (atr / price) * 100 if price > 0 else 0
                lev     = get_dynamic_leverage(symbol, atr_pct, score)

                new_setup = {
                    "coin": coin, "symbol": symbol,
                    "direction": best[2], "pattern": pattern_text,
                    "setup_score": score, "leverage": lev,
                    "scan_price": price, "tf_score": tf_score
                }

                # INSTANT signal — bypass queue
                if score >= INSTANT_SIGNAL_THRESHOLD:
                    if (
                        coin not in active_trades and
                        coin not in pending_signals and
                        len(active_trades) < MAX_ACTIVE_TRADES
                    ):
                        logger.info(f"⚡ INSTANT signal: {coin} | Score: {score}")
                        format_and_send(new_setup, coin, is_instant=True)

                # Normal signal — add to batch queue
                else:
                    if (
                        coin not in active_trades and
                        coin not in pending_signals and
                        len(active_trades) < MAX_ACTIVE_TRADES and
                        (coin not in hourly_queue or score > hourly_queue[coin]["setup_score"])
                    ):
                        hourly_queue[coin] = new_setup
                        logger.info(f"Queued: {coin} | {best[2]} | Score: {score} | TF: {tf_score}")

        except Exception as e:
            logger.error(f"Scan error {coin}: {e}", exc_info=True)

        time.sleep(DELAY_BETWEEN_COINS)

# ================= MAIN LOOP =================
def main():
    global last_batch_time, last_river_time, last_hourly_time, last_pnl_update_time

    load_active_trades()
    load_trade_history()
    load_journal()
    threading.Thread(target=poll_telegram, daemon=True).start()

    send_telegram(
        f"🚀 <b>{BOT_HEADER} Started Successfully</b>\n\n"
        f"✅ Scanner Running ({len(COINS)} coins)\n"
        f"✅ Queue Engine Running\n"
        f"✅ River Engine Running\n"
        f"✅ Telegram Connected\n"
        f"✅ HTF Tiered Alignment Active\n"
        f"✅ Funding Rate Filter Active\n"
        f"✅ Open Interest Filter Active\n"
        f"✅ Whale Detection Active\n"
        f"✅ Fear & Greed Filter Active\n"
        f"✅ Circuit Breaker Active\n"
        f"✅ Instant Signal Mode Active (Score ≥ {INSTANT_SIGNAL_THRESHOLD})\n"
        f"✅ Trailing SL + Partial TP Active\n\n"
        f"📌 Type /help for commands"
    )
    logger.info(f"Bot {BOT_VERSION} started with {len(COINS)} coins.")

    while True:
        try:
            btc_price  = get_price("BTCUSDT")
            btc_klines = get_klines("BTCUSDT", "1h", 100)
            btc_ema50  = calculate_ema([float(x[4]) for x in btc_klines], 50)

            if not btc_price or btc_ema50 is None:
                logger.warning("Could not determine BTC trend, skipping cycle.")
                time.sleep(60)
                continue

            btc_trend = 1 if btc_price > btc_ema50 else -1
            fng       = get_fear_greed_index()

            logger.info(
                f"BTC: {'BULL' if btc_trend == 1 else 'BEAR'} | "
                f"Price: {btc_price:.2f} | EMA50: {btc_ema50:.2f} | "
                f"F&G: {fng} | Losses today: {daily_losses}/{MAX_DAILY_LOSSES}"
            )

            scan_coins(btc_trend, fng)
            check_active_trades()
            expire_pending_signals()

            now = time.time()

            if (now - last_hourly_time) >= 3600:
                send_hourly_report()
                last_hourly_time = now

            if (now - last_pnl_update_time) >= 3600:
                send_live_pnl_update()
                last_pnl_update_time = now

            if (now - last_batch_time) >= BATCH_INTERVAL:
                send_hourly_batch()

            if (now - last_river_time) >= RIVER_INTERVAL:
                scan_river(now)

            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(60)

if __name__ == "__main__":
    main()
