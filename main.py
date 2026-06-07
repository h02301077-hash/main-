import requests
import time
import json
import os
import threading
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import Counter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("tsm_v32g.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TOKEN_HERE")
CHAT_ID        = os.getenv("CHAT_ID", "YOUR_CHAT_ID_HERE")
NEWS_API_KEY   = os.getenv("NEWS_API_KEY", "")

BINANCE_PRICE_URL   = "https://data-api.binance.vision/api/v3/ticker/price"
BINANCE_KLINE_URL   = "https://data-api.binance.vision/api/v3/klines"
BINANCE_AGG_URL     = "https://api.binance.com/api/v3/aggTrades"
BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
BINANCE_OI_URL      = "https://fapi.binance.com/futures/data/openInterestHist"

trade_lock = threading.Lock()
IST        = ZoneInfo("Asia/Kolkata")

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

active_trades             = {}
pending_signals           = {}
hourly_queue              = {}
sent_coins                = []
daily_losses              = 0
circuit_breaker_until     = None
last_reset_day            = datetime.now(IST).date()
trade_journal             = []
learning_notes            = []
coin_cooldowns            = {}
consecutive_loss_patterns = {}
price_alerts              = {}
market_memory = {
    "bull":     {"wins":0,"losses":0,"best_pattern":None},
    "bear":     {"wins":0,"losses":0,"best_pattern":None},
    "sideways": {"wins":0,"losses":0,"best_pattern":None}
}
pattern_stats = {p: {"signals":0,"wins":0,"losses":0,"total_pnl":0.0,"weight":1.0,
                     "bull_wr":0.0,"bear_wr":0.0,"sideways_wr":0.0} for p in [
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

SCAN_INTERVAL            = 300
BATCH_INTERVAL           = 1800
RIVER_INTERVAL           = 900
MIN_SETUP_SCORE          = 94
MIN_PRIMARY_SCORE        = 82
INSTANT_SIGNAL_THRESHOLD = 97
MIN_PROFIT_TARGET        = 15.0
SIGNAL_EXPIRY_MINUTES    = 120
INSTANT_EXPIRY_MINUTES   = 30
DELAY_BETWEEN_COINS      = 0.15
MAX_SIGNALS_PER_CYCLE    = 3
MAX_ACTIVE_TRADES        = 5
ATR_SL_MULTIPLIER        = 2.5
ATR_TP_MULTIPLIER        = 5.0
MAX_DAILY_LOSSES         = 3
CIRCUIT_BREAKER_MIN_LOSS = -5.0
WHALE_TRADE_THRESHOLD    = 500000
ATR_VOLATILITY_RATIO     = 3.0
CONSEC_LOSS_SUSPEND      = 5
MIN_SIGNALS_TO_SUSPEND   = 15
SUSPEND_HOURS            = 12
ADX_MIN_TREND            = 21
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
BOT_VERSION = "v32G"
BOT_NAME    = "TRADING SIGNAL MASTER"
BOT_HEADER  = f"⚙️ {BOT_NAME} {BOT_VERSION}"

def S(c="━",n=30): return c*n
def fmt_pnl(v): return ("🟢 " if v>=0 else "🔴 ")+f"{v:+.2f}%"

def save_active_trades():
    with trade_lock:
        try:
            s={k:{**v,"timestamp":v["timestamp"].isoformat(),
                  "expires_at":v["expires_at"].isoformat() if v.get("expires_at") else None}
               for k,v in active_trades.items()}
            with open("active_trades.json","w") as f: json.dump(s,f)
        except Exception as e: logger.error(f"save_active_trades: {e}")

def load_active_trades():
    global active_trades
    try:
        if os.path.exists("active_trades.json"):
            with open("active_trades.json") as f: data=json.load(f)
            active_trades={k:{**v,
                "timestamp":datetime.fromisoformat(v["timestamp"]),
                "expires_at":datetime.fromisoformat(v["expires_at"]) if v.get("expires_at") else None}
                for k,v in data.items()}
            logger.info(f"Loaded {len(active_trades)} active trades.")
    except Exception as e: logger.error(f"load_active_trades: {e}")

def save_trade_history():
    with trade_lock:
        try:
            with open("trades.json","w") as f: json.dump(pattern_stats,f)
        except Exception as e: logger.error(f"save_trade_history: {e}")

def load_trade_history():
    global pattern_stats
    try:
        if os.path.exists("trades.json"):
            with open("trades.json") as f: loaded=json.load(f)
            for p in pattern_stats:
                if p in loaded: pattern_stats[p]=loaded[p]
    except Exception as e: logger.error(f"load_trade_history: {e}")

def save_journal():
    try:
        with open("journal.json","w") as f: json.dump(trade_journal,f)
    except Exception as e: logger.error(f"save_journal: {e}")

def load_journal():
    global trade_journal
    try:
        if os.path.exists("journal.json"):
            with open("journal.json") as f: trade_journal=json.load(f)
        logger.info(f"Loaded {len(trade_journal)} journal entries.")
    except Exception as e: logger.error(f"load_journal: {e}")

def save_learning():
    try:
        with open("learning.json","w") as f:
            json.dump({"notes":learning_notes,"memory":market_memory,"clp":consecutive_loss_patterns},f)
    except Exception as e: logger.error(f"save_learning: {e}")

def load_learning():
    global learning_notes,market_memory,consecutive_loss_patterns
    try:
        if os.path.exists("learning.json"):
            with open("learning.json") as f: data=json.load(f)
            learning_notes=data.get("notes",[])
            market_memory.update(data.get("memory",{}))
            consecutive_loss_patterns=data.get("clp",{})
    except Exception as e: logger.error(f"load_learning: {e}")

def save_alerts():
    try:
        with open("alerts.json","w") as f: json.dump(price_alerts,f)
    except Exception as e: logger.error(f"save_alerts: {e}")

def load_alerts():
    global price_alerts
    try:
        if os.path.exists("alerts.json"):
            with open("alerts.json") as f: price_alerts=json.load(f)
    except Exception as e: logger.error(f"load_alerts: {e}")

def save_circuit_breaker():
    try:
        with open("cb.json","w") as f:
            json.dump({"daily_losses":daily_losses,
                       "circuit_breaker_until":circuit_breaker_until,
                       "date":str(last_reset_day)},f)
    except Exception as e: logger.error(f"save_cb: {e}")

def load_circuit_breaker():
    global daily_losses,circuit_breaker_until,last_reset_day
    try:
        if os.path.exists("cb.json"):
            with open("cb.json") as f: data=json.load(f)
            if data.get("date")==str(datetime.now(IST).date()):
                daily_losses=data.get("daily_losses",0)
                circuit_breaker_until=data.get("circuit_breaker_until")
    except Exception as e: logger.error(f"load_cb: {e}")

def format_price(p):
    if p>=1000:   return f"{p:.2f}"
    elif p>=1:    return f"{p:.4f}"
    elif p>=0.01: return f"{p:.6f}"
    else:         return f"{p:.8f}"

def get_ist_time():     return datetime.now(IST).strftime("%I:%M:%S %p IST")
def get_ist_datetime(): return datetime.now(IST)

def send_telegram(text, parse_mode="HTML", reply_markup=None):
    payload={"chat_id":CHAT_ID,"text":text,"parse_mode":parse_mode}
    if reply_markup: payload["reply_markup"]=reply_markup
    try:
        res=requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json=payload,timeout=15)
        if res.status_code!=200:
            logger.warning(f"Telegram [{res.status_code}]: {res.text[:200]}")
            if "parse" in res.text.lower() or "can't parse" in res.text.lower():
                payload2={"chat_id":CHAT_ID,"text":text}
                if reply_markup: payload2["reply_markup"]=reply_markup
                res2=requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                                   json=payload2,timeout=15)
                return res2.status_code==200
        return res.status_code==200
    except requests.RequestException as e:
        logger.error(f"Telegram error: {e}"); return False

def answer_callback(cbid, text="OK"):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                      json={"callback_query_id":cbid,"text":text},timeout=10)
    except Exception as e:
        logger.warning(f"answerCallback: {e}")

def get_price(symbol):
    try:
        res=requests.get(BINANCE_PRICE_URL,params={"symbol":symbol},timeout=10)
        return float(res.json()["price"]) if res.status_code==200 else None
    except Exception as e:
        logger.warning(f"get_price {symbol}: {e}"); return None

def get_klines(symbol,interval,limit=100):
    try:
        res=requests.get(BINANCE_KLINE_URL,
                         params={"symbol":symbol,"interval":interval,"limit":limit},timeout=10)
        return res.json() if res.status_code==200 else []
    except Exception as e:
        logger.warning(f"get_klines {symbol}: {e}"); return []

def calculate_ema(closes,period):
    if len(closes)<period: return None
    ema=sum(closes[:period])/period
    k=2.0/(period+1)
    for p in closes[period:]: ema=p*k+ema*(1-k)
    return ema

def calculate_rsi(closes,period=14):
    if len(closes)<period+1: return 50.0
    gains,losses=[],[]
    for i in range(1,len(closes)):
        d=closes[i]-closes[i-1]
        gains.append(max(0,d)); losses.append(max(0,-d))
    ag=sum(gains[-period:])/period; al=sum(losses[-period:])/period
    return 100.0-(100.0/(1+ag/al)) if al!=0 else 100.0

def calculate_atr(klines,period=14):
    if len(klines)<period+1: return 0.0
    trs=[]
    for i in range(1,len(klines)):
        h=float(klines[i][2]); l=float(klines[i][3]); pc=float(klines[i-1][4])
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(trs[-period:])/period

def calculate_adx(klines,period=14):
    if len(klines)<period*2+1: return 30.0
    try:
        highs=[float(k[2]) for k in klines]; lows=[float(k[3]) for k in klines]
        closes=[float(k[4]) for k in klines]
        pdm,mdm,trl=[],[],[]
        for i in range(1,len(klines)):
            hd=highs[i]-highs[i-1]; ld=lows[i-1]-lows[i]
            pdm.append(hd if hd>ld and hd>0 else 0)
            mdm.append(ld if ld>hd and ld>0 else 0)
            trl.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
        def smooth(data,p):
            s=sum(data[:p]); r=[s]
            for v in data[p:]: s=s-s/p+v; r.append(s)
            return r
        atr_s=smooth(trl,period); pdm_s=smooth(pdm,period); mdm_s=smooth(mdm,period)
        pdi=[100*p/a if a else 0 for p,a in zip(pdm_s,atr_s)]
        mdi=[100*m/a if a else 0 for m,a in zip(mdm_s,atr_s)]
        dx=[100*abs(p-m)/(p+m) if (p+m) else 0 for p,m in zip(pdi,mdi)]
        return sum(dx[-period:])/period if len(dx)>=period else 30.0
    except Exception: return 30.0

def calculate_supertrend(klines,period=10,multiplier=3.0):
    if len(klines)<period+1: return None
    try:
        highs=[float(k[2]) for k in klines]; lows=[float(k[3]) for k in klines]
        closes=[float(k[4]) for k in klines]
        atr=calculate_atr(klines,period)
        hl2=(highs[-1]+lows[-1])/2
        upper=hl2+multiplier*atr; lower=hl2-multiplier*atr
        price=closes[-1]; prev=closes[-2] if len(closes)>1 else price
        if price>lower and prev>lower: return "BUY"
        if price<upper and prev<upper: return "SELL"
        return "BUY" if price>hl2 else "SELL"
    except Exception: return None

def calculate_vwap(klines):
    try:
        tp=sum(((float(k[2])+float(k[3])+float(k[4]))/3)*float(k[5]) for k in klines)
        tv=sum(float(k[5]) for k in klines)
        return tp/tv if tv>0 else None
    except Exception: return None

def get_dol_signal(klines):
    try:
        highs=[float(k[2]) for k in klines[-30:]]; lows=[float(k[3]) for k in klines[-30:]]
        closes=[float(k[4]) for k in klines[-30:]]
        max_high=max(highs[-10:]); min_low=min(lows[-10:])
        eq_highs=sum(1 for h in highs[-10:] if abs(h-max_high)/max_high<0.003)
        eq_lows=sum(1 for l in lows[-10:] if abs(l-min_low)/min_low<0.003)
        last_range=highs[-1]-lows[-1]
        upper_wick=highs[-1]-max(closes[-1],float(klines[-1][1]))
        lower_wick=min(closes[-1],float(klines[-1][1]))-lows[-1]
        if eq_highs>=3 and eq_lows<2:   return "Liquidity ABOVE - sell sweep likely"
        elif eq_lows>=3 and eq_highs<2: return "Liquidity BELOW - buy sweep likely"
        elif upper_wick>last_range*0.6: return "Upper wick rejection - sellers strong"
        elif lower_wick>last_range*0.6: return "Lower wick rejection - buyers strong"
        else:                           return "No clear liquidity imbalance"
    except Exception: return "N/A"

def detect_rsi_divergence(closes):
    if len(closes)<10: return None
    try:
        prices=closes[-6:]
        rsi_vals=[calculate_rsi(closes[:i+1]) for i in range(len(closes)-6,len(closes))]
        if prices[-1]<prices[0] and rsi_vals[-1]>rsi_vals[0]: return "BULLISH_DIV"
        if prices[-1]>prices[0] and rsi_vals[-1]<rsi_vals[0]: return "BEARISH_DIV"
        return None
    except Exception: return None

def detect_supply_demand_zones(klines):
    zones={"demand":[],"supply":[]}
    try:
        closes=[float(k[4]) for k in klines]; opens=[float(k[1]) for k in klines]
        highs=[float(k[2]) for k in klines]; lows=[float(k[3]) for k in klines]
        for i in range(3,len(klines)-1):
            body=abs(closes[i]-opens[i])
            avg_body=sum(abs(closes[j]-opens[j]) for j in range(i-3,i))/3
            if avg_body==0: continue
            if closes[i]>opens[i] and body>avg_body*1.5:
                zones["demand"].append({"high":max(opens[i],closes[i-1]),"low":min(lows[i-1],lows[i-2])})
            if closes[i]<opens[i] and body>avg_body*1.5:
                zones["supply"].append({"high":max(highs[i-1],highs[i-2]),"low":min(opens[i],closes[i-1])})
    except Exception: pass
    return zones

def is_in_zone(price,direction,zones):
    key="demand" if direction=="BUY" else "supply"
    for zone in zones.get(key,[])[-5:]:
        if zone["low"]*0.995<=price<=zone["high"]*1.005:
            return True,f"{format_price(zone['low'])}-{format_price(zone['high'])}"
    return False,""

def detect_market_condition(btc_price,btc_klines):
    try:
        closes=[float(k[4]) for k in btc_klines]
        e20=calculate_ema(closes,20); e50=calculate_ema(closes,50)
        h20=max(closes[-20:]); l20=min(closes[-20:])
        rng=((h20-l20)/l20)*100 if l20>0 else 0
        if e20 and e50:
            if e20>e50*1.02 and btc_price>e20:   return "bull"
            elif e20<e50*0.98 and btc_price<e20: return "bear"
        return "sideways" if rng<5.0 else ("bull" if btc_price>(e50 or btc_price) else "bear")
    except Exception: return "sideways"

def is_good_trading_session():
    hour=datetime.now(IST).hour
    if DEAD_HOUR_START<=hour<DEAD_HOUR_END:
        logger.info(f"Dead session {hour}:xx IST"); return False
    return True

def get_smart_leverage(symbol,atr_pct,score):
    base=symbol.replace("USDT","")
    if base in LEV_TIER_1:   bl,hc=10,12
    elif base in LEV_TIER_2: bl,hc=6,8
    elif base in LEV_TIER_3: bl,hc=2,3
    elif atr_pct<2.0:        bl,hc=5,7
    elif atr_pct<4.0:        bl,hc=4,6
    else:                    bl,hc=3,5
    bonus=2 if score>=99 else 1 if score>=97 else 0
    return min(bl+bonus,hc)

def get_signal_grade(score,whale,oi_rising,tf_score,vol_ok,rsi_ok,funding_ok,st_ok,vwap_ok,zone_ok,adx_val):
    pts=0
    if score>=98:    pts+=3
    elif score>=96:  pts+=2
    else:            pts+=1
    if whale:        pts+=2
    if oi_rising:    pts+=2
    if tf_score==3:  pts+=2
    elif tf_score==2:pts+=1
    if vol_ok:       pts+=1
    if rsi_ok:       pts+=1
    if funding_ok:   pts+=1
    if st_ok:        pts+=2
    if vwap_ok:      pts+=1
    if zone_ok:      pts+=2
    if adx_val>=35:  pts+=1
    if pts>=14:      return "Grade A+"
    elif pts>=11:    return "Grade A"
    elif pts>=8:     return "Grade B"
    else:            return "Grade C"

def get_position_size_pct(grade):
    if "A+" in grade: return 10.0
    elif "A" in grade: return 7.0
    elif "B" in grade: return 5.0
    else:              return 3.0

def is_volume_confirmed(klines):
    vols=[float(k[5]) for k in klines]
    return len(vols)>=20 and vols[-1]>sum(vols[-20:])/20*1.05

def is_rsi_valid(closes,direction):
    rsi=calculate_rsi(closes)
    return not (direction=="BUY" and rsi>72) and not (direction=="SELL" and rsi<28)

def is_volatility_normal(klines):
    an=calculate_atr(klines,14); as_=calculate_atr(klines,50)
    return as_==0 or (an/as_)<=ATR_VOLATILITY_RATIO

def is_pattern_blacklisted(name):
    s=pattern_stats.get(name)
    if not s or s["signals"]<10: return False
    return (s["wins"]/s["signals"])*100<40

def is_pattern_suspended(name):
    d=consecutive_loss_patterns.get(name,{})
    if d.get("consecutive_losses",0)>=CONSEC_LOSS_SUSPEND:
        su=d.get("suspended_until")
        if su:
            try:
                if datetime.now(IST)<datetime.fromisoformat(su): return True
                consecutive_loss_patterns[name]["consecutive_losses"]=0
                consecutive_loss_patterns[name]["suspended_until"]=None
            except Exception: pass
    return False

def too_many_correlated_active():
    return sum(1 for c in active_trades if c in BTC_CORRELATED)>=2

def get_funding_rate(symbol):
    try:
        res=requests.get(BINANCE_FUNDING_URL,params={"symbol":symbol,"limit":1},timeout=10)
        return float(res.json()[0]["fundingRate"]) if res.status_code==200 and res.json() else None
    except Exception as e:
        logger.warning(f"funding {symbol}: {e}"); return None

def is_funding_favorable(symbol,direction):
    rate=get_funding_rate(symbol)
    if rate is None: return True
    if direction=="BUY"  and rate>0.002:  return False
    if direction=="SELL" and rate<-0.002: return False
    return True

def get_oi_trend(symbol):
    try:
        res=requests.get(BINANCE_OI_URL,params={"symbol":symbol,"period":"15m","limit":5},timeout=10)
        if res.status_code==200 and len(res.json())>=2:
            d=res.json()
            return float(d[-1]["sumOpenInterest"])>float(d[-2]["sumOpenInterest"])
        return None
    except Exception as e:
        logger.warning(f"OI {symbol}: {e}"); return None

def has_whale_activity(symbol):
    try:
        res=requests.get(BINANCE_AGG_URL,params={"symbol":symbol,"limit":20},timeout=10)
        if res.status_code==200:
            for t in res.json():
                if float(t["p"])*float(t["q"])>WHALE_TRADE_THRESHOLD: return True
        return False
    except Exception as e:
        logger.warning(f"whale {symbol}: {e}"); return False

def get_fear_greed_index():
    try:
        res=requests.get("https://api.alternative.me/fng/?limit=1",timeout=10)
        return int(res.json()["data"][0]["value"]) if res.status_code==200 else 50
    except Exception as e:
        logger.warning(f"F&G: {e}"); return 50

def is_sentiment_valid(direction,fng):
    return not (direction=="BUY" and fng<20) and not (direction=="SELL" and fng>80)

def get_htf_trend(symbol,interval="1h"):
    try:
        klines=get_klines(symbol,interval,50)
        if not klines or len(klines)<50: return 0
        closes=[float(k[4]) for k in klines]
        e20=calculate_ema(closes,20); e50=calculate_ema(closes,50)
        if e20 and e50: return 1 if e20>e50 else -1
        return 0
    except Exception as e:
        logger.warning(f"HTF {symbol} {interval}: {e}"); return 0

def get_timeframe_score(symbol,direction):
    di=1 if direction=="BUY" else -1
    h4=get_htf_trend(symbol,"4h"); h1=get_htf_trend(symbol,"1h")
    if h4!=0 and h4!=di: return -1
    score=0
    if h4==di: score+=2
    if h1==di: score+=1
    return score

def get_structure_sl(klines,direction,entry,atr):
    lows=[float(k[3]) for k in klines[-20:]]; highs=[float(k[2]) for k in klines[-20:]]
    min_dist=entry*MIN_SL_PCT
    if direction=="BUY":
        sl=min(min(lows)*0.998,entry-atr*ATR_SL_MULTIPLIER)
        return min(sl,entry-min_dist)
    sl=max(max(highs)*1.002,entry+atr*ATR_SL_MULTIPLIER)
    return max(sl,entry+min_dist)

def check_circuit_breaker():
    global daily_losses,circuit_breaker_until,last_reset_day
    today=datetime.now(IST).date()
    if today!=last_reset_day:
        daily_losses=0; circuit_breaker_until=None; last_reset_day=today
        save_circuit_breaker(); return False
    if circuit_breaker_until:
        try:
            until_dt=datetime.fromisoformat(circuit_breaker_until)
            if datetime.now(IST)>=until_dt:
                daily_losses=0; circuit_breaker_until=None
                save_circuit_breaker()
                send_telegram(f"✅ <b>{BOT_HEADER}</b>\nCircuit Breaker RESET - scanning resumed!")
                return False
            return True
        except Exception:
            circuit_breaker_until=None; return False
    return daily_losses>=MAX_DAILY_LOSSES

def increment_daily_losses(pnl):
    global daily_losses,circuit_breaker_until
    if pnl>CIRCUIT_BREAKER_MIN_LOSS:
        logger.info(f"Small loss {pnl:.2f}% - not counted"); return
    daily_losses+=1
    if daily_losses>=MAX_DAILY_LOSSES:
        midnight=(datetime.now(IST)+timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
        circuit_breaker_until=midnight.isoformat()
        save_circuit_breaker()
        send_telegram(f"🚨 <b>{BOT_HEADER}</b>\nCIRCUIT BREAKER ACTIVE\n3 big losses today.\nResumes at midnight IST.")

def is_btc_crashing():
    try:
        klines=get_klines("BTCUSDT","1h",5)
        if not klines or len(klines)<4: return False
        now=float(klines[-1][4]); h4=float(klines[-4][4])
        drop=((now-h4)/h4)*100
        if drop<-5.0: logger.info(f"BTC crashed {drop:.1f}% in 4h"); return True
        return False
    except Exception: return False

def get_adjusted_score(pattern_name,base_score,market_condition):
    stats=pattern_stats.get(pattern_name,{})
    signals=stats.get("signals",0)
    if signals<5: return base_score
    overall_wr=(stats["wins"]/signals)*100
    mc_wr=stats.get(f"{market_condition}_wr",overall_wr)
    weight=stats.get("weight",1.0)
    if signals>=20:   pf=0.6
    elif signals>=10: pf=0.4
    else:             pf=0.2
    adjusted=(base_score*(1-pf)+mc_wr*pf)*weight
    return min(round(adjusted,1),99.0)

def get_all_pattern_scores(patterns,market_condition):
    scored=[]
    for name,base_score,direction in patterns:
        adj=get_adjusted_score(name,base_score,market_condition)
        scored.append((name,adj,direction,base_score))
    scored.sort(key=lambda x:x[1],reverse=True)
    return scored

def learn_from_trade(coin,pattern,result,pnl,mc,tf_score):
    global learning_notes,market_memory,consecutive_loss_patterns
    if result=="WIN": market_memory[mc]["wins"]+=1
    else:             market_memory[mc]["losses"]+=1
    wins_by_pat={}
    for e in trade_journal:
        if e.get("market_condition")==mc and e.get("result")=="WIN":
            p=e.get("pattern","?"); wins_by_pat[p]=wins_by_pat.get(p,0)+1
    if wins_by_pat:
        market_memory[mc]["best_pattern"]=max(wins_by_pat,key=wins_by_pat.get)
    if pattern not in consecutive_loss_patterns:
        consecutive_loss_patterns[pattern]={"consecutive_losses":0,"suspended_until":None}
    if result=="LOSS":
        consecutive_loss_patterns[pattern]["consecutive_losses"]+=1
        cl=consecutive_loss_patterns[pattern]["consecutive_losses"]
        sigs=pattern_stats.get(pattern,{}).get("signals",0)
        if cl>=CONSEC_LOSS_SUSPEND and sigs>=MIN_SIGNALS_TO_SUSPEND:
            su=(datetime.now(IST)+timedelta(hours=SUSPEND_HOURS)).isoformat()
            consecutive_loss_patterns[pattern]["suspended_until"]=su
            send_telegram(f"🧠 <b>{BOT_HEADER}</b>\nPattern suspended: {pattern}\n{cl} consecutive losses.")
    else:
        consecutive_loss_patterns[pattern]["consecutive_losses"]=0
        consecutive_loss_patterns[pattern]["suspended_until"]=None
    if pattern in pattern_stats:
        s=pattern_stats[pattern]; sigs=s.get("signals",0)
        if sigs>=3:
            wr=(s["wins"]/sigs)*100
            if wr>=70:   s["weight"]=min(s["weight"]+0.1,1.5)
            elif wr<40:  s["weight"]=max(s["weight"]-0.15,0.5)
            mc_trades=[t for t in trade_journal if t.get("pattern")==pattern and t.get("market_condition")==mc]
            mc_wins=sum(1 for t in mc_trades if t["result"]=="WIN")
            mc_wr=(mc_wins/len(mc_trades)*100) if mc_trades else 50.0
            s[f"{mc}_wr"]=round(mc_wr,1)
    stats=pattern_stats.get(pattern,{}); sigs2=stats.get("signals",0); note=None
    if sigs2>=5:
        wr=(stats["wins"]/sigs2)*100
        if result=="LOSS" and wr<45:
            note=f"Pattern '{pattern}' only {wr:.1f}% WR - consider avoiding in {mc} market."
        elif result=="WIN" and wr>70:
            note=f"Pattern '{pattern}' strong - {wr:.1f}% WR in {mc} market."
    if note and note not in learning_notes:
        learning_notes.append(note)
        if len(learning_notes)>100: learning_notes=learning_notes[-100:]
    save_learning()

def get_crypto_news():
    headlines=[]
    try:
        res=requests.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest",timeout=10)
        if res.status_code==200:
            for a in res.json().get("Data",[])[:6]:
                t=a.get("title","")[:80]; src=a.get("source_info",{}).get("name","")
                if t: headlines.append(f"- {t} ({src})")
    except Exception as e: logger.warning(f"news: {e}")
    fng=get_fear_greed_index()
    fng_lbl="Extreme Fear" if fng<=25 else "Fear" if fng<=45 else "Neutral" if fng<=55 else "Greed" if fng<=75 else "Extreme Greed"
    prices=[]
    for sym,lbl in [("BTCUSDT","BTC"),("ETHUSDT","ETH"),("SOLUSDT","SOL"),("BNBUSDT","BNB")]:
        p=get_price(sym)
        if p: prices.append(f"{lbl}: ${format_price(p)}")
    msg=f"<b>{BOT_HEADER} Market Update</b>\n"
    msg+=f"{S()}\n"
    msg+=f"Fear & Greed: {fng} - {fng_lbl}\n"
    msg+="\n".join(prices)+"\n"
    if headlines:
        msg+=f"\n{S()}\n<b>Latest News:</b>\n\n"
        msg+="\n\n".join(headlines[:5])
    msg+=f"\n{S()}\n{get_ist_time()}"
    return msg

def run_backtest(symbol):
    try:
        klines=get_klines(symbol,"15m",1000)
        if not klines or len(klines)<100: return f"Not enough data for {symbol}"
        results={"WIN":0,"LOSS":0,"SKIP":0}; cond_res={"bull":{"W":0,"L":0},"bear":{"W":0,"L":0},"sideways":{"W":0,"L":0}}
        total_pnl=0.0; window=60
        for i in range(window,len(klines)-10):
            wk=klines[i-window:i]; price=float(klines[i][4])
            closes=[float(k[4]) for k in wk]; e20=calculate_ema(closes,20); e50=calculate_ema(closes,50)
            rng=((max(closes[-20:])-min(closes[-20:]))/min(closes[-20:]))*100 if min(closes[-20:])>0 else 0
            if e20 and e50:
                if e20>e50*1.02:   cond="bull"
                elif e20<e50*0.98: cond="bear"
                else:              cond="sideways" if rng<5 else ("bull" if price>e50 else "bear")
            else: cond="sideways"
            bt=1 if (e20 and e50 and e20>e50) else -1
            found=detect_patterns(symbol,wk,price,bt)
            if not found: continue
            best=max(found,key=lambda x:x[1])
            if best[1]<MIN_PRIMARY_SCORE: continue
            atr=calculate_atr(wk)
            if atr==0: continue
            entry=price; direction=best[2]
            sl=entry-atr*ATR_SL_MULTIPLIER if direction=="BUY" else entry+atr*ATR_SL_MULTIPLIER
            tp=entry+atr*ATR_TP_MULTIPLIER if direction=="BUY" else entry-atr*ATR_TP_MULTIPLIER
            hit="SKIP"
            for j in range(i+1,min(i+96,len(klines))):
                fh=float(klines[j][2]); fl=float(klines[j][3])
                if direction=="BUY":
                    if fh>=tp: hit="WIN";  break
                    if fl<=sl: hit="LOSS"; break
                else:
                    if fl<=tp: hit="WIN";  break
                    if fh>=sl: hit="LOSS"; break
            if hit=="SKIP": results["SKIP"]+=1; continue
            results[hit]+=1; cond_res[cond]["W" if hit=="WIN" else "L"]+=1
            pnl=(abs(tp-entry)/entry)*100*5 if hit=="WIN" else -(abs(sl-entry)/entry)*100*5
            total_pnl+=pnl
        total=results["WIN"]+results["LOSS"]; wr=(results["WIN"]/total*100) if total>0 else 0
        r=f"<b>{BOT_HEADER} Backtest: {symbol}</b>\n{S()}\n"
        r+=f"Trades: {total} | Wins: {results['WIN']} | Losses: {results['LOSS']}\n"
        r+=f"Win Rate: {wr:.1f}% | PnL: {fmt_pnl(total_pnl)}\n\nBy Market:\n"
        for cond,res in cond_res.items():
            ct=res["W"]+res["L"]; wr2=(res["W"]/ct*100) if ct>0 else 0
            r+=f"  {cond}: {res['W']}W/{res['L']}L ({wr2:.1f}%)\n"
        return r
    except Exception as e: return f"Backtest failed: {e}"

def get_active_trades_text():
    if not active_trades: return f"<b>{BOT_HEADER}</b>\nNo active trades."
    text=f"<b>{BOT_HEADER} Active Trades ({len(active_trades)})</b>\n{S()}\n\n"
    for coin,t in active_trades.items():
        sl_pct=abs(t['entry']-t['sl'])/t['entry']*100; tp_pct=abs(t['tp']-t['entry'])/t['entry']*100
        text+=f"🔹 <b>{coin}</b> {t['direction']} | {t['leverage']}x\n"
        text+=f"   Entry: {format_price(t['entry'])}\n"
        text+=f"   TP: {format_price(t['tp'])} (+{tp_pct:.1f}%) | SL: {format_price(t['sl'])} (-{sl_pct:.1f}%)\n\n"
    return text

def get_pattern_stats_text():
    text=f"<b>{BOT_HEADER} Pattern Performance</b>\n{S()}\n\n"
    for pat,s in sorted(pattern_stats.items(),key=lambda x:x[1]["signals"],reverse=True)[:10]:
        if s["signals"]>0:
            wr=(s["wins"]/s["signals"])*100; flag="🔴" if wr<40 else "🟡" if wr<60 else "🟢"
            susp=" SUSPENDED" if is_pattern_suspended(pat) else ""
            text+=f"{flag} <b>{pat}</b>{susp}\nSignals:{s['signals']} WR:{wr:.1f}% PnL:{fmt_pnl(s['total_pnl'])}\n\n"
    return text

def get_10day_summary_text():
    today=datetime.now(IST).date(); text=f"<b>{BOT_HEADER} Last 10 Days</b>\n{S()}\n\n"
    ow=ol=0; op=0.0
    for days_ago in range(9,-1,-1):
        day=today-timedelta(days=days_ago); dt=[j for j in trade_journal if j.get("date")==str(day)]
        w=sum(1 for t in dt if t["result"]=="WIN"); l=sum(1 for t in dt if t["result"]=="LOSS")
        total=w+l; pnl=sum(t["pnl"] for t in dt); wr=(w/total*100) if total>0 else 0
        ow+=w; ol+=l; op+=pnl; ds=day.strftime("%d %b")
        if total==0: text+=f"⚪ <b>{ds}</b> - No trades\n"
        else:
            em="✅" if w>l else "❌" if l>w else "➖"
            text+=f"{em} <b>{ds}</b>: {w}W/{l}L WR:{wr:.0f}% {fmt_pnl(pnl)}\n"
    ot=ow+ol; owr=(ow/ot*100) if ot>0 else 0
    text+=f"\n{S()}\n10 Day Total: {ow}W {ol}L | WR:{owr:.1f}% | {fmt_pnl(op)}"
    return text

def get_streak_text():
    if not trade_journal: return f"<b>{BOT_HEADER}</b>\nNo trades yet."
    st=trade_journal[-1]["result"]; sc=0
    for t in reversed(trade_journal):
        if t["result"]==st: sc+=1
        else: break
    em="🔥" if st=="WIN" else "❄️"
    return f"{em} <b>{BOT_HEADER} Streak</b>\n{'Winning' if st=='WIN' else 'Losing'}: {sc} trades"

def get_best_text():
    if not trade_journal: return f"<b>{BOT_HEADER}</b>\nNo data yet."
    cs={}; ps2={}
    for t in trade_journal:
        c=t["coin"]
        if c not in cs: cs[c]={"W":0,"L":0,"pnl":0.0}
        cs[c]["W" if t["result"]=="WIN" else "L"]+=1; cs[c]["pnl"]+=t["pnl"]
        p=t["pattern"]
        if p not in ps2: ps2[p]={"W":0,"L":0}
        ps2[p]["W" if t["result"]=="WIN" else "L"]+=1
    text=f"<b>{BOT_HEADER} Best Performers</b>\n{S()}\n\nTop Coins:\n"
    sc=sorted(cs.items(),key=lambda x:(x[1]["W"]/(x[1]["W"]+x[1]["L"])) if (x[1]["W"]+x[1]["L"])>0 else 0,reverse=True)[:3]
    for i,(c,s) in enumerate(sc,1):
        tot=s["W"]+s["L"]; wr=(s["W"]/tot*100) if tot>0 else 0
        text+=f"{i}. <b>{c}</b> {wr:.1f}% WR {fmt_pnl(s['pnl'])}\n"
    text+="\nTop Patterns:\n"
    sp=sorted(ps2.items(),key=lambda x:(x[1]["W"]/(x[1]["W"]+x[1]["L"])) if (x[1]["W"]+x[1]["L"])>0 else 0,reverse=True)[:3]
    for i,(p,s) in enumerate(sp,1):
        tot=s["W"]+s["L"]; wr=(s["W"]/tot*100) if tot>0 else 0
        text+=f"{i}. <b>{p}</b> {wr:.1f}% WR ({tot} trades)\n"
    return text

def get_risk_text():
    if not active_trades: return f"<b>{BOT_HEADER}</b>\nNo active trades."
    text=f"<b>{BOT_HEADER} Risk Exposure</b>\n{S()}\n\n"; total_risk=0.0
    for coin,t in active_trades.items():
        rp=abs(t["entry"]-t["sl"])/t["entry"]*100*t["leverage"]; total_risk+=rp
        text+=f"{coin}: {rp:.1f}% max loss\n"
    text+=f"\nTotal Risk: {total_risk:.1f}% | Active: {len(active_trades)}/{MAX_ACTIVE_TRADES}"
    return text

def get_learning_text():
    if not learning_notes: return f"<b>{BOT_HEADER}</b>\nNo insights yet."
    text=f"<b>{BOT_HEADER} Learning</b>\n{S()}\n\nMarket Memory:\n"
    for cond in ["bull","bear","sideways"]:
        mem=market_memory[cond]; tot=mem["wins"]+mem["losses"]
        wr=(mem["wins"]/tot*100) if tot>0 else 0
        text+=f"  {cond}: {mem['wins']}W/{mem['losses']}L ({wr:.1f}%) Best:{mem['best_pattern'] or 'N/A'}\n"
    text+=f"\nLatest Insights:\n"
    for note in learning_notes[-8:]: text+=f"• {note}\n"
    return text

def get_journal_text():
    if not trade_journal: return f"<b>{BOT_HEADER}</b>\nNo trades yet."
    recent=trade_journal[-10:][::-1]
    text=f"<b>{BOT_HEADER} Trade Journal</b>\n{S()}\n\n"
    for t in recent:
        em="✅" if t.get("result")=="WIN" else "🔴"
        text+=f"{em} <b>{t.get('coin','?')}</b> {t.get('direction','?')} | {fmt_pnl(t.get('pnl',0))} | {t.get('duration','?')}\n"
        text+=f"   {t.get('pattern','?')}\n\n"
    total=len(trade_journal); wins=sum(1 for t in trade_journal if t.get("result")=="WIN")
    wr=(wins/total*100) if total>0 else 0
    text+=f"Total: {total} | WR: {wr:.1f}%"
    return text

def get_patterns_ranked_text():
    text=f"<b>{BOT_HEADER} All Patterns Ranked</b>\n{S()}\n\n"
    all_pats=[]
    for pat,s in pattern_stats.items():
        sigs=s.get("signals",0); wr=(s["wins"]/sigs*100) if sigs>0 else 0
        w=s.get("weight",1.0); adj=get_adjusted_score(pat,80,"bull")
        all_pats.append((pat,sigs,wr,w,adj))
    all_pats.sort(key=lambda x:x[4],reverse=True)
    for i,(pat,sigs,wr,w,adj) in enumerate(all_pats,1):
        flag="🔴" if wr<40 and sigs>=5 else "🟢" if wr>=60 else "🟡"
        susp=" SUSP" if is_pattern_suspended(pat) else ""
        trend="UP" if w>1.0 else "DN" if w<1.0 else "="
        text+=f"{i:2}. {flag} <b>{pat}</b>{susp}\n    Signals:{sigs} WR:{wr:.1f}% Weight:{w:.2f}({trend})\n\n"
    return text

def get_trend_label(ema20,ema50,price,label):
    if not ema20 or not ema50: return "Neutral"
    diff_pct=((ema20-ema50)/ema50)*100
    if price>ema20>ema50:
        if diff_pct>3:   return "Strong Uptrend"
        elif diff_pct>1: return "Uptrend"
        else:            return "Weak Uptrend"
    elif price<ema20<ema50:
        if diff_pct<-3:  return "Strong Downtrend"
        elif diff_pct<-1:return "Downtrend"
        else:            return "Weak Downtrend"
    elif price>ema50: return "Ranging Above EMA50"
    else:             return "Ranging Below EMA50"

def cmd_trend(coin_input):
    coin=coin_input.upper().replace("USDT","").strip()
    symbol=coin+"USDT"; price=get_price(symbol)
    if not price: return f"Could not get price for {coin}"
    tfs=[("1d","Daily "),("8h","8 Hour"),("4h","4 Hour"),("1h","1 Hour"),("15m","15 Min")]
    results=[]; bull_c=bear_c=0
    for tf,label in tfs:
        klines=get_klines(symbol,tf,60)
        if not klines or len(klines)<50: results.append((label,"No data",50,0)); continue
        closes=[float(k[4]) for k in klines]
        e20=calculate_ema(closes,20); e50=calculate_ema(closes,50)
        rsi=calculate_rsi(closes); adx=calculate_adx(klines)
        trend=get_trend_label(e20,e50,price,label)
        if "Uptrend" in trend:   bull_c+=1
        if "Downtrend" in trend: bear_c+=1
        results.append((label,trend,rsi,adx))
    if bull_c>=4:   bias="STRONGLY BULLISH"
    elif bull_c>=3: bias="BULLISH"
    elif bear_c>=4: bias="STRONGLY BEARISH"
    elif bear_c>=3: bias="BEARISH"
    else:           bias="MIXED/SIDEWAYS"
    klines_4h=get_klines(symbol,"4h",30); s1=r1=0
    if klines_4h and len(klines_4h)>=5:
        highs=[float(k[2]) for k in klines_4h]; lows=[float(k[3]) for k in klines_4h]
        closes=[float(k[4]) for k in klines_4h]
        pivot=(highs[-2]+lows[-2]+closes[-2])/3
        r1=2*pivot-lows[-2]; s1=2*pivot-highs[-2]
    text=f"<b>{BOT_HEADER} Trend: {coin}</b>\n"
    text+=f"Price: {format_price(price)}\n{S()}\n\n"
    for label,trend,rsi,adx in results:
        em="🟢" if "Up" in trend else "🔴" if "Down" in trend else "🟡"
        text+=f"{em} <b>{label}</b>: {trend}\n"
    rsi_1h=results[3][2] if len(results)>3 else 50
    adx_1h=results[3][3] if len(results)>3 else 0
    dol=get_dol_signal(get_klines(symbol,"1h",35) or [])
    text+=f"\n{S()}\n"
    text+=f"Overall Bias: <b>{bias}</b>\n"
    if s1 and r1:
        text+=f"Support: {format_price(s1)} | Resistance: {format_price(r1)}\n"
    text+=f"RSI(1h): {rsi_1h:.1f} | ADX(1h): {adx_1h:.1f}\n"
    text+=f"DOL: {dol}\n{S()}\n{get_ist_time()}"
    return text

def cmd_market():
    btc=get_price("BTCUSDT"); eth=get_price("ETHUSDT"); sol=get_price("SOLUSDT")
    btc_klines=get_klines("BTCUSDT","1h",50); btc_trend="N/A"
    if btc_klines and len(btc_klines)>=50:
        closes=[float(k[4]) for k in btc_klines]
        e20=calculate_ema(closes,20); e50=calculate_ema(closes,50)
        btc_trend=get_trend_label(e20,e50,btc,"1h") if btc else "N/A"
    scan_list=["BTC","ETH","BNB","SOL","XRP","ADA","AVAX","DOT","LINK","NEAR",
               "INJ","SUI","APT","ARB","OP","ATOM","PEPE","WIF","BONK","DOGE"]
    gainers=[]; losers=[]
    for coin in scan_list:
        try:
            klines=get_klines(coin+"USDT","1d",3)
            if klines and len(klines)>=2:
                prev=float(klines[-2][4]); curr=float(klines[-1][4])
                chg=((curr-prev)/prev)*100 if prev>0 else 0
                if chg>0: gainers.append((coin,chg))
                else:     losers.append((coin,chg))
        except Exception: continue
    gainers.sort(key=lambda x:x[1],reverse=True); losers.sort(key=lambda x:x[1])
    fng=get_fear_greed_index()
    fng_lbl="Extreme Fear" if fng<=25 else "Fear" if fng<=45 else "Neutral" if fng<=55 else "Greed" if fng<=75 else "Extreme Greed"
    text=f"<b>{BOT_HEADER} Market Overview</b>\n{S()}\n\n"
    text+=f"Fear & Greed: {fng} - {fng_lbl}\n"
    if btc: text+=f"BTC: ${format_price(btc)}\n"
    if eth: text+=f"ETH: ${format_price(eth)}\n"
    if sol: text+=f"SOL: ${format_price(sol)}\n"
    text+=f"BTC Trend: {btc_trend}\n\n"
    text+=f"Top Gainers (24h):\n"
    for coin,chg in gainers[:5]: text+=f"  {coin}: +{chg:.2f}%\n"
    text+=f"\nTop Losers (24h):\n"
    for coin,chg in losers[:5]: text+=f"  {coin}: {chg:.2f}%\n"
    text+=f"\n{S()}\n{get_ist_time()}"
    return text

def cmd_compare(coins_str):
    coins=[c.upper().replace("USDT","") for c in coins_str.split()[:4]]
    if not coins: return "Usage: /compare BTC ETH SOL"
    text=f"<b>{BOT_HEADER} Compare</b>\n{S()}\n\n"
    for coin in coins:
        symbol=coin+"USDT"; price=get_price(symbol)
        if not price: text+=f"{coin}: Not found\n\n"; continue
        klines=get_klines(symbol,"4h",60); trend="N/A"; rsi=50.0; adx=0.0
        if klines and len(klines)>=50:
            closes=[float(k[4]) for k in klines]
            e20=calculate_ema(closes,20); e50=calculate_ema(closes,50)
            rsi=calculate_rsi(closes); adx=calculate_adx(klines)
            trend=get_trend_label(e20,e50,price,"4h")
        em="🟢" if "Up" in trend else "🔴" if "Down" in trend else "🟡"
        text+=f"{em} <b>{coin}</b>: {format_price(price)}\n4h:{trend} RSI:{rsi:.1f} ADX:{adx:.1f}\n\n"
    return text

def cmd_scan_manual(btc_trend,fng,market_condition):
    send_telegram(f"🔄 <b>{BOT_HEADER}</b>\nScanning {len(COINS[:30])} coins...")
    results=[]
    for coin in COINS[:30]:
        try:
            symbol=coin+"USDT"; price=get_price(symbol); klines=get_klines(symbol,"15m",100)
            if not price or not klines: continue
            found=detect_patterns(symbol,klines,price,btc_trend)
            if not found: continue
            scored=get_all_pattern_scores(found,market_condition)
            if not scored: continue
            best=scored[0]; adj_score=min(best[1]+min(len(scored)*0.5,3),99)
            tf_score=get_timeframe_score(symbol,best[2])
            if tf_score==-1: continue
            results.append({"coin":coin,"direction":best[2],"score":adj_score,"pattern":best[0],"tf_score":tf_score})
        except Exception: continue
        time.sleep(0.1)
    if not results: return f"<b>{BOT_HEADER}</b>\nNo qualifying setups right now."
    results.sort(key=lambda x:x["score"],reverse=True)
    text=f"<b>{BOT_HEADER} Scan Results</b>\n{S()}\n\n"
    for r in results[:5]:
        em="🟢" if r["direction"]=="BUY" else "🔴"
        tf="⭐" if r["tf_score"]==3 else "✅" if r["tf_score"]==2 else "⚠️"
        text+=f"{em} <b>{r['coin']}</b> {r['direction']} Score:{r['score']:.1f} {tf}\n"
        text+=f"   {r['pattern']}\n\n"
    text+=f"Market:{market_condition} F&G:{fng}\n{get_ist_time()}"
    return text

def expire_pending_signals():
    now=get_ist_datetime()
    expired=[c for c,s in list(pending_signals.items()) if s.get("expires_at") and now>s["expires_at"]]
    for coin in expired:
        del pending_signals[coin]
        send_telegram(f"⏰ <b>{BOT_HEADER}</b>\nSignal expired: <b>{coin}</b>")

def check_price_alerts():
    triggered=[]
    for sym,alert in list(price_alerts.items()):
        price=get_price(sym+"USDT")
        if not price: continue
        if alert["direction"]=="above" and price>=alert["price"]:
            send_telegram(f"🔔 <b>{BOT_HEADER}</b>\n{sym} is above {format_price(alert['price'])}\nNow: {format_price(price)}")
            triggered.append(sym)
        elif alert["direction"]=="below" and price<=alert["price"]:
            send_telegram(f"🔔 <b>{BOT_HEADER}</b>\n{sym} is below {format_price(alert['price'])}\nNow: {format_price(price)}")
            triggered.append(sym)
    for sym in triggered: del price_alerts[sym]
    if triggered: save_alerts()

def detect_patterns(symbol,klines,price,btc_trend):
    if len(klines)<50: return []
    closes=[float(k[4]) for k in klines]; opens=[float(k[1]) for k in klines]
    highs=[float(k[2]) for k in klines]; lows=[float(k[3]) for k in klines]
    vols=[float(k[5]) for k in klines]; avg_vol=sum(vols[-20:])/20
    rsi=calculate_rsi(closes); ema20=calculate_ema(closes,20); ema50=calculate_ema(closes,50)
    adx=calculate_adx(klines)
    if ((max(highs[-20:])-min(lows[-20:]))/price)*100<1.8: return []
    if adx<ADX_MIN_TREND: return []
    p=[]; sup=min(lows[-30:-1]); res=max(highs[-30:-1])
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
    mom=(closes[-1]-closes[-3])/closes[-3]*100 if len(closes)>3 else 0
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

def update_trailing_sl(coin,trade,price):
    trail=abs(trade["tp"]-trade["entry"])*0.3
    if trade["direction"]=="BUY":
        new_sl=price-trail
        if new_sl>trade["sl"]: active_trades[coin]["sl"]=new_sl; save_active_trades()
    else:
        new_sl=price+trail
        if new_sl<trade["sl"]: active_trades[coin]["sl"]=new_sl; save_active_trades()

def check_profit_milestones(coin,trade,price,pnl):
    milestones=trade.get("milestones_sent",[])
    ep=trade["entry"]; tp=trade["tp"]; direction=trade["direction"]
    if direction=="BUY":
        sl_10=format_price(ep); sl_20=format_price(ep+(tp-ep)*0.5); sl_35=format_price(ep+(tp-ep)*0.75)
    else:
        sl_10=format_price(ep); sl_20=format_price(ep-(ep-tp)*0.5); sl_35=format_price(ep-(ep-tp)*0.75)
    if 10<=pnl<20 and "p10" not in milestones:
        active_trades[coin].setdefault("milestones_sent",[]).append("p10")
        save_active_trades()
        send_telegram(f"🎯 <b>{BOT_HEADER} {coin} +10% PROFIT</b>\n{S()}\n"
                      f"Move SL to entry: {sl_10}\n"
                      f"Trade is now risk-free!\n"
                      f"Current PnL: {fmt_pnl(pnl)}")
    elif 20<=pnl<35 and "p20" not in milestones:
        active_trades[coin].setdefault("milestones_sent",[]).append("p20")
        save_active_trades()
        send_telegram(f"🎯 <b>{BOT_HEADER} {coin} +20% PROFIT</b>\n{S()}\n"
                      f"Move SL to: {sl_20}\n"
                      f"Locking in 10% minimum profit!\n"
                      f"Current PnL: {fmt_pnl(pnl)}")
    elif pnl>=35 and "p35" not in milestones:
        active_trades[coin].setdefault("milestones_sent",[]).append("p35")
        save_active_trades()
        send_telegram(f"🚀 <b>{BOT_HEADER} {coin} +35% PROFIT</b>\n{S()}\n"
                      f"Move SL to: {sl_35}\n"
                      f"Locking in 25% minimum profit!\n"
                      f"Current PnL: {fmt_pnl(pnl)}")

def format_and_send(setup,coin,is_river=False,is_instant=False,market_condition="bull"):
    global sent_coins,coin_cooldowns
    if check_circuit_breaker(): return False
    if not is_good_trading_session(): return False
    live_price=get_price(setup["symbol"])
    if not live_price: return False
    entry=live_price
    if abs(entry-setup["scan_price"])/setup["scan_price"]>0.01:
        logger.info(f"{coin} rejected - drifted"); return False
    klines_15m=get_klines(setup["symbol"],"15m",100)
    klines_1h=get_klines(setup["symbol"],"1h",50)
    if not klines_15m: return False
    closes=[float(x[4]) for x in klines_15m]
    atr_1h=calculate_atr(klines_1h) if len(klines_1h)>=15 else calculate_atr(klines_15m)
    atr_pct=(atr_1h/entry)*100 if entry>0 else 0
    vol_ok=is_volume_confirmed(klines_15m)
    rsi_ok=is_rsi_valid(closes,setup["direction"])
    funding_ok=is_funding_favorable(setup["symbol"],setup["direction"])
    if not vol_ok:
        logger.info(f"{coin} rejected - volume"); return False
    if not rsi_ok:
        logger.info(f"{coin} rejected - RSI"); return False
    if not is_volatility_normal(klines_15m):
        logger.info(f"{coin} rejected - volatility"); return False
    if not funding_ok:
        logger.info(f"{coin} rejected - funding"); return False
    st_15m=calculate_supertrend(klines_15m,ST_PERIOD,ST_MULTIPLIER)
    st_1h=calculate_supertrend(klines_1h,ST_PERIOD,ST_MULTIPLIER) if klines_1h else st_15m
    st_ok=(st_15m==setup["direction"]) and (st_1h==setup["direction"])
    if not st_ok:
        logger.info(f"{coin} rejected - SuperTrend ({st_15m}/{st_1h})"); return False
    vwap=calculate_vwap(klines_15m); vwap_ok=False; vwap_label="N/A"
    if vwap:
        if setup["direction"]=="BUY" and entry>vwap:    vwap_ok=True; vwap_label=f"Above {format_price(vwap)}"
        elif setup["direction"]=="SELL" and entry<vwap: vwap_ok=True; vwap_label=f"Below {format_price(vwap)}"
        else: vwap_label=f"{'Below' if setup['direction']=='BUY' else 'Above'} {format_price(vwap)}"
    zones=detect_supply_demand_zones(klines_15m)
    zone_ok,zone_label=is_in_zone(entry,setup["direction"],zones)
    div=detect_rsi_divergence(closes)
    oi_rising=get_oi_trend(setup["symbol"])
    oi_label="Rising" if oi_rising else "Falling" if oi_rising is False else "N/A"
    whale=has_whale_activity(setup["symbol"])
    adx_val=calculate_adx(klines_15m)
    dol=get_dol_signal(klines_15m)
    tf_score=setup.get("tf_score",get_timeframe_score(setup["symbol"],setup["direction"]))
    lev=get_smart_leverage(setup["symbol"],atr_pct,setup["setup_score"])
    sl=get_structure_sl(klines_15m,setup["direction"],entry,atr_1h)
    tp=entry+atr_1h*ATR_TP_MULTIPLIER if setup["direction"]=="BUY" else entry-atr_1h*ATR_TP_MULTIPLIER
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
    eta=max(30,min(eta,1440)); setup["eta_minutes"]=eta
    expiry_minutes=INSTANT_EXPIRY_MINUTES if is_instant else SIGNAL_EXPIRY_MINUTES
    expiry_time=get_ist_datetime()+timedelta(minutes=expiry_minutes)
    expiry_str=expiry_time.strftime("%I:%M %p IST")
    mom=(closes[-1]-closes[-3])/closes[-3]*100
    rsi_val=calculate_rsi(closes)
    grade=get_signal_grade(setup["setup_score"],whale,oi_rising,tf_score,vol_ok,rsi_ok,funding_ok,st_ok,vwap_ok,zone_ok,adx_val)
    pos_size=get_position_size_pct(grade)
    sl_pct=abs(entry-sl)/entry*100; tp_pct=abs(tp-entry)/entry*100
    rr_ratio=tp_pct/sl_pct if sl_pct>0 else 0
    tf_map={3:"4h + 1h STRONG",2:"4h ALIGNED",1:"1h ONLY",0:"COUNTER-TREND"}
    tf_label=tf_map.get(tf_score,"N/A")
    cond_em={"bull":"Bullish","bear":"Bearish","sideways":"Sideways"}.get(market_condition,"")
    dir_em="BUY" if setup["direction"]=="BUY" else "SELL"
    if is_instant: sig_type="INSTANT SIGNAL"
    elif is_river: sig_type="RIVER SIGNAL"
    else:          sig_type="VERIFIED SETUP"
    msg =f"🔥 <b>{sig_type} - {coin}</b>\n"
    msg+=f"<b>{BOT_HEADER}</b>\n"
    msg+=f"{S()}\n"
    msg+=f"<b>{dir_em}</b> | Leverage: <b>{lev}x</b> | {cond_em}\n"
    msg+=f"Grade: <b>{grade}</b> | Score: <b>{setup['setup_score']:.0f}/100</b>\n"
    msg+=f"{S()}\n"
    msg+=f"Entry:   <code>{format_price(entry)}</code>\n"
    msg+=f"Target:  <code>{format_price(tp)}</code>  (+{tp_pct:.2f}%)\n"
    msg+=f"Stop:    <code>{format_price(sl)}</code>  (-{sl_pct:.2f}%)\n"
    msg+=f"Profit:  <b>{profit_target:.1f}%</b> | RR: <b>1:{rr_ratio:.1f}</b>\n"
    msg+=f"Size:    <b>{pos_size:.0f}% of capital</b>\n"
    msg+=f"{S()}\n"
    msg+=f"Pattern: {setup['pattern']}\n"
    msg+=f"RSI: {rsi_val:.1f} | ADX: {adx_val:.1f} | Mom: {mom:+.2f}%\n"
    msg+=f"TF: {tf_label}\n"
    msg+=f"SuperTrend: {'Confirmed' if st_ok else 'Mixed'} ({st_15m}/{st_1h})\n"
    msg+=f"VWAP: {vwap_label}\n"
    msg+=f"OI: {oi_label} | Whale: {'Yes' if whale else 'No'}\n"
    if zone_ok: msg+=f"Zone: In {'demand' if setup['direction']=='BUY' else 'supply'} zone\n"
    if div=="BULLISH_DIV": msg+=f"RSI Divergence: Bullish\n"
    elif div=="BEARISH_DIV": msg+=f"RSI Divergence: Bearish\n"
    msg+=f"DOL: {dol}\n"
    msg+=f"{S()}\n"
    msg+=f"ETA: ~{eta} mins | Expires: {expiry_str}\n"
    msg+=f"ATR(1h): {format_price(atr_1h)}\n"
    msg+=f"{S()}\n"
    msg+=f"Milestone Plan:\n"
    if setup["direction"]=="BUY":
        msg+=f"  +10% - Move SL to {format_price(entry)}\n"
        msg+=f"  +20% - Move SL to {format_price(entry+(tp-entry)*0.5)}\n"
        msg+=f"  +35% - Move SL to {format_price(entry+(tp-entry)*0.75)}\n"
    else:
        msg+=f"  +10% - Move SL to {format_price(entry)}\n"
        msg+=f"  +20% - Move SL to {format_price(entry-(entry-tp)*0.5)}\n"
        msg+=f"  +35% - Move SL to {format_price(entry-(entry-tp)*0.75)}\n"
    msg+=f"{S()}\n{get_ist_time()}"
    setup.update({"entry":entry,"sl":sl,"tp":tp,"timestamp":get_ist_datetime(),
                  "expires_at":expiry_time,"reversal_alerted":False,"breakeven_sent":False,
                  "partial_tp_taken":False,"milestones_sent":[],"tf_score":tf_score,
                  "market_condition":market_condition,"eta_minutes":eta})
    pending_signals[coin]=setup
    reply_markup={"inline_keyboard":[[
        {"text":"✅ Activate Trade","callback_data":f"ACTIVATE_{coin}"},
        {"text":"❌ Ignore","callback_data":f"IGNORE_{coin}"}
    ]]}
    result=send_telegram(msg,reply_markup=reply_markup)
    if result:
        sent_coins.append(coin)
        coin_cooldowns[coin]=get_ist_datetime()+timedelta(minutes=eta)
        logger.info(f"Signal sent: {coin}|{setup['direction']}|Score:{setup['setup_score']}|ETA:{eta}m")
        return True
    else:
        if coin in pending_signals: del pending_signals[coin]
        return False

def check_active_trades():
    for coin,trade in list(active_trades.items()):
        price=get_price(trade["symbol"])
        if not price: continue
        if trade["direction"]=="BUY":
            pnl=((price-trade["entry"])/trade["entry"])*100*trade["leverage"]
        else:
            pnl=((trade["entry"]-price)/trade["entry"])*100*trade["leverage"]
        update_trailing_sl(coin,trade,price)
        check_profit_milestones(coin,trade,price,pnl)
        if not trade.get("reversal_alerted",False):
            klines=get_klines(trade["symbol"],"15m",20)
            if klines:
                closes=[float(x[4]) for x in klines]; ema20=calculate_ema(closes,20)
                if ema20:
                    rev=((trade["direction"]=="BUY" and price<ema20*0.995) or
                         (trade["direction"]=="SELL" and price>ema20*1.005))
                    if rev:
                        send_telegram(f"⚠️ <b>{BOT_HEADER}</b>\nReversal alert: {coin}\nPrice broke EMA20")
                        active_trades[coin]["reversal_alerted"]=True; save_active_trades()
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
                    pattern_stats[primary]["signals"]+=1
                    pattern_stats[primary]["total_pnl"]+=pnl
                    pattern_stats[primary]["wins" if hit=="WIN" else "losses"]+=1
                increment_daily_losses(pnl)
                if hit=="LOSS":
                    coin_cooldowns[coin]=get_ist_datetime()+timedelta(hours=4)
                duration=""
                if trade.get("timestamp"):
                    mins=int((get_ist_datetime()-trade["timestamp"]).total_seconds()/60)
                    duration=f"{mins} mins"
                mc=trade.get("market_condition","bull")
                trade_journal.append({"date":str(datetime.now(IST).date()),"coin":coin,
                    "direction":trade["direction"],"pattern":primary,
                    "entry":trade["entry"],"exit":price,"pnl":pnl,"result":hit,
                    "duration":duration,"tf_score":trade.get("tf_score",0),"market_condition":mc})
                save_journal(); learn_from_trade(coin,primary,hit,pnl,mc,trade.get("tf_score",0))
            em="✅" if hit=="WIN" else "🛑"
            send_telegram(f"{em} <b>{BOT_HEADER} {coin} {hit}</b>\n{S()}\n"
                          f"Entry: {format_price(trade['entry'])} to Exit: {format_price(price)}\n"
                          f"Pattern: {primary} | Duration: {duration}\nPnL: {fmt_pnl(pnl)}")
            del active_trades[coin]; save_active_trades(); save_trade_history()

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
                    cb=update["callback_query"]
                    data=cb.get("data","")
                    cbid=cb.get("id","")
                    answer_callback(cbid,"Processing...")
                    logger.info(f"Callback received: data={data} pending={list(pending_signals.keys())}")
                    if data and "_" in data:
                        action=data.split("_",1)[0]
                        coin=data.split("_",1)[1]
                        if action=="ACTIVATE":
                            if coin in pending_signals:
                                lp=get_price(pending_signals[coin].get("symbol",coin+"USDT"))
                                if lp and lp>0: pending_signals[coin]["entry"]=lp
                                pending_signals[coin]["breakeven_sent"]=False
                                pending_signals[coin]["partial_tp_taken"]=False
                                pending_signals[coin]["reversal_alerted"]=False
                                pending_signals[coin]["milestones_sent"]=[]
                                pending_signals[coin]["timestamp"]=get_ist_datetime()
                                pending_signals[coin]["expires_at"]=None
                                with trade_lock:
                                    active_trades[coin]=pending_signals[coin]
                                save_active_trades()
                                t=active_trades[coin]
                                ep=t.get("entry",0); sl_p=t.get("sl",0); tp_p=t.get("tp",0)
                                lev=t.get("leverage",5); dirn=t.get("direction","?"); pat=t.get("pattern","?")
                                sl_pct=abs(ep-sl_p)/ep*100 if ep>0 else 0
                                tp_pct=abs(tp_p-ep)/ep*100 if ep>0 else 0
                                rr=round(tp_pct/sl_pct,1) if sl_pct>0 else 0
                                if dirn=="BUY":
                                    sl_10=format_price(ep); sl_20=format_price(ep+(tp_p-ep)*0.5); sl_35=format_price(ep+(tp_p-ep)*0.75)
                                else:
                                    sl_10=format_price(ep); sl_20=format_price(ep-(ep-tp_p)*0.5); sl_35=format_price(ep-(ep-tp_p)*0.75)
                                send_telegram(
                                    f"🚀 <b>{BOT_HEADER} {coin} ACTIVATED</b>\n"
                                    f"{S()}\n"
                                    f"{dirn} | {lev}x | RR 1:{rr}\n"
                                    f"Entry:  {format_price(ep)}\n"
                                    f"Target: {format_price(tp_p)} (+{tp_pct:.1f}%)\n"
                                    f"Stop:   {format_price(sl_p)} (-{sl_pct:.1f}%)\n"
                                    f"Pattern: {pat}\n"
                                    f"{S()}\n"
                                    f"Milestone Plan:\n"
                                    f"+10% Move SL to {sl_10}\n"
                                    f"+20% Move SL to {sl_20}\n"
                                    f"+35% Move SL to {sl_35}\n"
                                    f"{S()}\n"
                                    f"Set your trade on CoinDCX now!\n"
                                    f"{get_ist_time()}"
                                )
                                del pending_signals[coin]
                                logger.info(f"ACTIVATED: {coin}|{dirn}|Entry:{ep}|{lev}x")
                            else:
                                send_telegram(f"⏰ <b>{BOT_HEADER}</b>\nSignal for {coin} expired.\nWait for next signal.")
                                logger.warning(f"ACTIVATE failed: {coin} not in pending={list(pending_signals.keys())}")
                        elif action=="IGNORE":
                            if coin in pending_signals: del pending_signals[coin]
                            send_telegram(f"❌ <b>{BOT_HEADER}</b>\n{coin} signal ignored.")
                elif "message" in update:
                    txt=update["message"].get("text","").strip().lower()
                    if   txt=="/trades":   send_telegram(get_active_trades_text())
                    elif txt=="/pending":
                        if pending_signals:
                            msg=f"<b>{BOT_HEADER} Pending ({len(pending_signals)})</b>\n{S()}\n\n"
                            for c,s in pending_signals.items():
                                exp=s.get("expires_at"); exp_str=exp.strftime("%I:%M %p") if exp else "N/A"
                                msg+=f"{c} {s.get('direction','?')} Score:{s.get('setup_score',0):.0f} Exp:{exp_str}\n"
                            send_telegram(msg)
                        else: send_telegram(f"<b>{BOT_HEADER}</b>\nNo pending signals.")
                    elif txt=="/stats":    send_telegram(get_pattern_stats_text())
                    elif txt=="/summary":  send_telegram(get_10day_summary_text())
                    elif txt=="/streak":   send_telegram(get_streak_text())
                    elif txt=="/best":     send_telegram(get_best_text())
                    elif txt=="/risk":     send_telegram(get_risk_text())
                    elif txt=="/learn":    send_telegram(get_learning_text())
                    elif txt=="/journal":  send_telegram(get_journal_text())
                    elif txt=="/patterns": send_telegram(get_patterns_ranked_text())
                    elif txt=="/news":
                        send_telegram(f"<b>{BOT_HEADER}</b>\nFetching news...")
                        send_telegram(get_crypto_news())
                    elif txt=="/market":   send_telegram(cmd_market())
                    elif txt=="/cb":
                        if check_circuit_breaker():
                            send_telegram(f"🔴 <b>{BOT_HEADER}</b>\nCircuit Breaker ACTIVE\nLosses: {daily_losses}/{MAX_DAILY_LOSSES}")
                        else:
                            send_telegram(f"🟢 <b>{BOT_HEADER}</b>\nCircuit Breaker OK\nLosses: {daily_losses}/{MAX_DAILY_LOSSES}")
                    elif txt.startswith("/trend"):
                        parts=txt.split(); coin2=parts[1].upper() if len(parts)>1 else "BTC"
                        send_telegram(cmd_trend(coin2))
                    elif txt.startswith("/compare"):
                        parts=txt.split(maxsplit=1); coins_str=parts[1].upper() if len(parts)>1 else "BTC ETH SOL"
                        send_telegram(cmd_compare(coins_str))
                    elif txt=="/scan":
                        btc_p=get_price("BTCUSDT"); btc_k=get_klines("BTCUSDT","1h",50)
                        bt_e50=calculate_ema([float(x[4]) for x in btc_k],50) if btc_k else None
                        bt=1 if (btc_p and bt_e50 and btc_p>bt_e50) else -1
                        fng2=get_fear_greed_index(); mc2=detect_market_condition(btc_p,btc_k) if btc_p and btc_k else "sideways"
                        send_telegram(cmd_scan_manual(bt,fng2,mc2))
                    elif txt.startswith("/alert "):
                        parts=txt.split()
                        if len(parts)>=4:
                            try:
                                sym=parts[1].upper(); target=float(parts[2]); direction=parts[3].lower()
                                price_alerts[sym]={"price":target,"direction":direction}; save_alerts()
                                send_telegram(f"🔔 Alert set: {sym} {direction} {format_price(target)}")
                            except Exception: send_telegram("Usage: /alert BTC 95000 above")
                        else: send_telegram("Usage: /alert BTC 95000 above")
                    elif txt=="/alerts":
                        if price_alerts:
                            msg=f"<b>{BOT_HEADER} Alerts</b>\n{S()}\n\n"
                            for sym,a in price_alerts.items(): msg+=f"{sym}: {a['direction']} {format_price(a['price'])}\n"
                            send_telegram(msg)
                        else: send_telegram(f"<b>{BOT_HEADER}</b>\nNo alerts set.")
                    elif txt.startswith("/backtest"):
                        parts=txt.split(); bc=(parts[1].upper() if len(parts)>1 else "BTC")+"USDT"
                        send_telegram(f"Running backtest for {bc}...")
                        send_telegram(run_backtest(bc))
                    elif txt in ("/start","/help","/menu"):
                        send_telegram(
                            f"<b>{BOT_HEADER}</b>\n{S()}\n\n"
                            f"/trades    - Active trades\n"
                            f"/pending   - Pending signals\n"
                            f"/stats     - Pattern stats\n"
                            f"/summary   - 10 day summary\n"
                            f"/streak    - Win/loss streak\n"
                            f"/best      - Top performers\n"
                            f"/risk      - Risk exposure\n"
                            f"/learn     - Bot insights\n"
                            f"/journal   - Trade journal\n"
                            f"/patterns  - All patterns ranked\n"
                            f"/news      - Crypto news\n"
                            f"/market    - Market overview\n"
                            f"/scan      - Manual scan\n"
                            f"/trend BTC - Trend analysis\n"
                            f"/compare BTC ETH - Compare coins\n"
                            f"/cb        - Circuit breaker\n"
                            f"/alert BTC 95000 above - Price alert\n"
                            f"/alerts    - View alerts\n"
                            f"/backtest BTC - Backtest\n"
                        )
        except requests.RequestException as e: logger.error(f"Poll network: {e}")
        except Exception as e:                 logger.error(f"Poll error: {e}",exc_info=True)
        time.sleep(2)

def send_hourly_report():
    r=f"<b>{BOT_HEADER} Hourly Report</b>\n{get_ist_time()}\n{S()}\n\n"
    r+=f"Active: {len(active_trades)} | Pending: {len(pending_signals)}\n"
    r+=f"Circuit Breaker: {'ACTIVE' if check_circuit_breaker() else 'OK'}\n\n"
    r+=get_pattern_stats_text()
    send_telegram(r)

def send_live_pnl_update():
    if not active_trades: return
    total_pnl=0.0; wins=losses=0
    msg=f"<b>{BOT_HEADER} Live PnL</b>\n{get_ist_time()}\n{S()}\n\n"
    for coin,t in active_trades.items():
        price=get_price(t["symbol"])
        if not price: continue
        pnl=(((price-t["entry"])/t["entry"])*100*t["leverage"] if t["direction"]=="BUY"
             else ((t["entry"]-price)/t["entry"])*100*t["leverage"])
        total_pnl+=pnl
        if pnl>=3: wins+=1
        elif pnl<=-3: losses+=1
        msg+=f"{coin} {t['direction']} | {fmt_pnl(pnl)}\n"
    total=wins+losses; wr=(wins/total*100) if total>0 else 0
    msg+=f"\n{S()}\nTotal: {fmt_pnl(total_pnl)} | WR: {wr:.1f}%"
    send_telegram(msg)


def generate_weekly_insight():
    today = datetime.now(IST).date()
    wt = [j for j in trade_journal
          if (today - datetime.strptime(j["date"], "%Y-%m-%d").date()).days < 7]
    if not wt: return "Not enough data for weekly insight yet."
    wins   = [t for t in wt if t["result"] == "WIN"]
    losses = [t for t in wt if t["result"] == "LOSS"]
    total  = len(wt)
    wr     = (len(wins) / total * 100) if total > 0 else 0
    day_wins = {}
    for t in wins:
        d = t["date"]; day_wins[d] = day_wins.get(d, 0) + 1
    best_day  = max(day_wins, key=day_wins.get) if day_wins else None
    wp        = [t["pattern"] for t in wins]
    lp        = [t["pattern"] for t in losses]
    best_pat  = Counter(wp).most_common(1)[0][0]  if wp  else None
    worst_pat = Counter(lp).most_common(1)[0][0]  if lp  else None
    sw_losses = sum(1 for t in losses if t.get("market_condition") == "sideways")
    msg  = f"AI Weekly Insight:\n"
    msg += f"{len(wins)}W / {len(losses)}L | WR: {wr:.1f}%\n"
    if best_day:  msg += f"Best day: {best_day}\n"
    if best_pat:  msg += f"Best pattern: {best_pat}\n"
    if worst_pat: msg += f"Most losses from: {worst_pat}\n"
    if sw_losses >= 2:
        msg += f"{sw_losses} losses in sideways — reduce size when BTC ranges\n"
    if wr >= 70:   msg += "Excellent week!"
    elif wr >= 50: msg += "Decent week. Stay disciplined."
    else:          msg += "Tough week. Review learning notes."
    return msg

def send_weekly_report():
    today=datetime.now(IST).date(); week=[today-timedelta(days=i) for i in range(6,-1,-1)]
    wins=losses=0; total_pnl=0.0
    msg=f"<b>{BOT_HEADER} Weekly Report</b>\n{today.strftime('%d %b %Y')}\n{S()}\n\n"
    for day in week:
        dt=[j for j in trade_journal if j.get("date")==str(day)]
        w=sum(1 for t in dt if t["result"]=="WIN"); l=sum(1 for t in dt if t["result"]=="LOSS")
        pnl=sum(t["pnl"] for t in dt); wins+=w; losses+=l; total_pnl+=pnl
        em="✅" if w>l else "❌" if l>w else "⚪"
        msg+=f"{em} {day.strftime('%a %d')}: {w}W/{l}L {fmt_pnl(pnl)}\n"
    total=wins+losses; wr=(wins/total*100) if total>0 else 0
    msg+=f"\n{S()}\nTotal: {wins}W/{losses}L | WR:{wr:.1f}% | {fmt_pnl(total_pnl)}"
    msg+=f"\n\n{generate_weekly_insight()}"
    send_telegram(msg)

def scan_river(now,market_condition):
    global last_river_time
    try:
        if "RIVER" not in active_trades and "RIVER" not in pending_signals:
            price=get_price("RIVERUSDT"); klines=get_klines("RIVERUSDT","15m",100)
            if not price or not klines or len(klines)<50: return
            found=detect_patterns("RIVERUSDT",klines,price,1)+detect_patterns("RIVERUSDT",klines,price,-1)
            seen=set(); unique=[]
            for pat in found:
                if (pat[0],pat[2]) not in seen: seen.add((pat[0],pat[2])); unique.append(pat)
            if unique:
                best=max(unique,key=lambda x:x[1])
                if best[1]<MIN_PRIMARY_SCORE: return
                confirmed=list(dict.fromkeys([x[0] for x in unique]))
                primary=best[0]; extras=[p for p in confirmed if p!=primary]
                pt=primary+(" + "+" + ".join(extras[:2]) if extras else "")
                score=min(best[1]+min(len(unique)*0.5,2),99)
                if score>=82:
                    atr=calculate_atr(klines); atr_pct=(atr/price)*100 if price>0 else 0
                    setup={"coin":"RIVER","symbol":"RIVERUSDT","direction":best[2],"pattern":pt,
                           "setup_score":score,"leverage":get_smart_leverage("RIVERUSDT",atr_pct,score),
                           "scan_price":price}
                    format_and_send(setup,"RIVER",is_river=True,is_instant=score>=INSTANT_SIGNAL_THRESHOLD,market_condition=market_condition)
        last_river_time=now
    except Exception as e: logger.error(f"River: {e}",exc_info=True)

def scan_coins(btc_trend,fng,market_condition):
    btc_crashing=is_btc_crashing(); signals_this_cycle=0
    for coin in COINS:
        if signals_this_cycle>=MAX_SIGNALS_PER_CYCLE: break
        try:
            if coin in coin_cooldowns:
                if get_ist_datetime()<coin_cooldowns[coin]:
                    logger.info(f"Skip {coin} - cooldown until {coin_cooldowns[coin].strftime('%H:%M')}"); continue
                else: del coin_cooldowns[coin]
            symbol=coin+"USDT"; price=get_price(symbol); klines=get_klines(symbol,"15m")
            if not price or not klines: continue
            found=detect_patterns(symbol,klines,price,btc_trend)
            if not found: continue
            scored=get_all_pattern_scores(found,market_condition)
            signal_sent=False
            for direction in ["BUY","SELL"]:
                if signal_sent: break
                dir_pats=[p for p in scored if p[2]==direction]
                if not dir_pats: continue
                best_pat=dir_pats[0]; primary=best_pat[0]; adj_score=best_pat[1]; base_s=best_pat[3]
                if base_s<MIN_PRIMARY_SCORE:                                   continue
                if is_pattern_blacklisted(primary):                             continue
                if is_pattern_suspended(primary):                               continue
                if not is_sentiment_valid(direction,fng):                       continue
                if btc_crashing and direction=="BUY":                           continue
                if coin in BTC_CORRELATED and too_many_correlated_active():     continue
                tf_score=get_timeframe_score(symbol,direction)
                if tf_score==-1: logger.info(f"Skip {coin} {direction} - counter-trend"); continue
                extras=[p[0] for p in dir_pats[1:3]]
                pt=primary+(" + "+" + ".join(extras) if extras else "")
                confirm_bonus=min(len(dir_pats)*0.5,3.0)
                score=min(adj_score+confirm_bonus,99)
                if score<MIN_SETUP_SCORE: continue
                atr=calculate_atr(klines); atr_pct=(atr/price)*100 if price>0 else 0
                lev=get_smart_leverage(symbol,atr_pct,score)
                setup={"coin":coin,"symbol":symbol,"direction":direction,"pattern":pt,
                       "setup_score":score,"leverage":lev,"scan_price":price,
                       "market_condition":market_condition,"tf_score":tf_score}
                if (coin not in active_trades and coin not in pending_signals and len(active_trades)<MAX_ACTIVE_TRADES):
                    is_inst=score>=INSTANT_SIGNAL_THRESHOLD
                    logger.info(f"{'INSTANT' if is_inst else 'SIGNAL'}: {coin}|{direction}|Score:{score:.1f}|{primary}")
                    if format_and_send(setup,coin,is_instant=is_inst,market_condition=market_condition):
                        signal_sent=True; signals_this_cycle+=1
        except Exception as e: logger.error(f"Scan {coin}: {e}",exc_info=True)
        time.sleep(DELAY_BETWEEN_COINS)

def main():
    global last_batch_time,last_river_time,last_hourly_time,last_pnl_update_time,last_weekly_report_day
    load_active_trades(); load_trade_history(); load_journal()
    load_learning(); load_alerts(); load_circuit_breaker()
    threading.Thread(target=poll_telegram,daemon=True).start()
    logger.info(f"{BOT_NAME} {BOT_VERSION} starting...")
    send_telegram(
        f"<b>{BOT_NAME} {BOT_VERSION}</b>\n"
        f"Smart - Fast - Accurate - AI\n"
        f"{S()}\n\n"
        f"Scanner: {len(COINS)} coins\n"
        f"Min Score: {MIN_SETUP_SCORE}\n"
        f"ADX Min: {ADX_MIN_TREND}\n"
        f"Expiry: {SIGNAL_EXPIRY_MINUTES} mins\n"
        f"Circuit Breaker: losses under -5% only\n"
        f"ETA-based cooldown per coin\n"
        f"All filters active\n"
        f"{S()}\n"
        f"Type /help for commands\n"
        f"{get_ist_time()}"
    )
    while True:
        try:
            btc_price=get_price("BTCUSDT"); btc_klines=get_klines("BTCUSDT","1h",100)
            btc_ema50=calculate_ema([float(x[4]) for x in btc_klines],50) if btc_klines else None
            if not btc_price or btc_ema50 is None:
                logger.warning("BTC data unavailable"); time.sleep(60); continue
            btc_trend=1 if btc_price>btc_ema50 else -1
            fng=get_fear_greed_index()
            market_condition=detect_market_condition(btc_price,btc_klines)
            logger.info(f"BTC:{'BULL' if btc_trend==1 else 'BEAR'}|Market:{market_condition}|F&G:{fng}|Losses:{daily_losses}/{MAX_DAILY_LOSSES}|CB:{'ACTIVE' if check_circuit_breaker() else 'OK'}")
            scan_coins(btc_trend,fng,market_condition)
            check_active_trades()
            expire_pending_signals()
            check_price_alerts()
            now=time.time()
            if (now-last_hourly_time)>=3600:          send_hourly_report();   last_hourly_time=now
            if (now-last_pnl_update_time)>=3600:      send_live_pnl_update(); last_pnl_update_time=now
            if (now-last_river_time)>=RIVER_INTERVAL:  scan_river(now,market_condition); last_river_time=now
            today=datetime.now(IST).date()
            if today.weekday()==6 and last_weekly_report_day!=today:
                send_weekly_report(); last_weekly_report_day=today
            time.sleep(SCAN_INTERVAL)
        except Exception as e:
            logger.error(f"Main loop: {e}",exc_info=True); time.sleep(60)

if __name__=="__main__":
    main()
