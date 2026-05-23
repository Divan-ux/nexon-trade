# server.py
import os, json, random, time, threading, feedparser, re
from datetime import datetime, timedelta
import requests
import yfinance as yf
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
from geotext import GeoText

from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS

SECRET_KEY = "nexon_pulse_secret"
app = Flask(__name__, static_folder='.', template_folder='.')
app.config['SECRET_KEY'] = SECRET_KEY
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', ping_timeout=60, ping_interval=25)

# ─── RSS FEEDS ─────────────────────────────────────────
RSS_FEEDS = {
    "cybersecurity": [
        "https://feeds.feedburner.com/TheHackersNews",
        "https://krebsonsecurity.com/feed/",
        "https://www.darkreading.com/rss.xml",
        "https://www.bleepingcomputer.com/feed/",
        "https://www.zdnet.com/topic/security/rss.xml",
        "https://www.wired.com/feed/category/security/latest/rss",
        "https://www.cisa.gov/cybersecurity-advisories/rss.xml",
        "https://threatpost.com/feed/",
        "https://www.securityweek.com/rss.xml",
        "https://www.cyberscoop.com/feed/",
        "https://www.infosecurity-magazine.com/feed/",
        "https://www.csoonline.com/feed/",
        "https://www.scmagazine.com/feed/",
        "https://www.helpnetsecurity.com/feed/",
        "https://www.itsecurityguru.com/feed/",
        "https://www.tripwire.com/state-of-security/feed",
        "https://www.schneier.com/blog/atom.xml",
        "https://www.recordedfuture.com/feed",
        "https://www.digitalshadows.com/blog/feed",
        "https://www.fireeye.com/blog/feed",
    ],
    "technology": [
        "https://www.cnbc.com/id/19854910/device/rss/rss.html",
        "https://www.theverge.com/rss/index.xml",
        "https://www.techcrunch.com/feed/",
        "https://www.engadget.com/rss.xml",
        "https://www.wired.com/feed/category/business/latest/rss",
        "https://arstechnica.com/feed/",
        "https://www.technologyreview.com/feed/",
        "https://www.cnet.com/rss/news/",
        "https://www.bbc.com/news/technology/rss.xml",
        "https://www.reuters.com/technology/rss",
        "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/technology/rss.xml",
    ],
    "business_finance": [
        "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "https://www.reuters.com/business/rss",
        "https://www.marketwatch.com/rss/topstories",
        "https://www.ft.com/?format=rss",
        "https://www.economist.com/feeds/print-sections/77/business.xml",
        "https://www.forbes.com/business/feed/",
    ],
    "government": [
        "https://www.cisa.gov/cybersecurity-advisories/rss.xml",
        "https://www.nsa.gov/Press-Room/News-Highlights/Feed/",
        "https://www.fbi.gov/feeds/news/rss.xml",
        "https://www.whitehouse.gov/feed/",
        "https://www.state.gov/feed/",
    ],
    "vulnerability": [
        "https://nvd.nist.gov/feeds/xml/cve/misc/nvd-rss.xml",
        "https://www.exploit-db.com/rss.xml",
        "https://cve.mitre.org/cve/data/updates/rss.xml",
    ],
    "ai_ml": [
        "https://www.artificialintelligence-news.com/feed/",
        "https://www.aitrends.com/feed/",
        "https://machinelearningmastery.com/feed/",
        "https://www.deeplearning.ai/the-batch/feed/",
        "https://www.technologyreview.com/topic/artificial-intelligence/feed/",
    ]
}

# ─── STOCK & CRYPTO TICKERS ─────────────────────────────────────────
STOCK_TICKERS = [
    "MSFT", "GOOGL", "AMZN", "AAPL", "META", "NVDA", "TSLA",
    "CRWD", "PANW", "S", "FTNT", "OKTA", "ZS", "NET", "CHKP", "TENB", "VRNS",
    "QLYS", "RPD", "DDOG", "FSLY", "AKAM", "CLBT", "OSPN", "ATEN", "EVTC",
    "ADBE", "ORCL", "CRM", "NOW", "SNOW", "PLTR", "U", "PATH", "MNDY", "GTLB",
    "DBX", "BOX", "ZM", "DOCN", "SENT", "RBRK",
    "INTC", "AMD", "CSCO", "IBM"
][:56]

CRYPTO_TICKERS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD", "ADA-USD", "AVAX-USD", "DOGE-USD", "LINK-USD"]

# ─── FOREX PAIRS ─────────────────────────
FOREX_PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X",
    "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X"
]

def fetch_forex_rates():
    forex = {}
    for pair in FOREX_PAIRS:
        try:
            ticker = yf.Ticker(pair)
            info = ticker.history(period="1d", interval="1m")
            if not info.empty:
                price = info['Close'].iloc[-1]
                forex[pair] = {"price": round(price, 5)}
            else:
                forex[pair] = {"price": None}
        except:
            forex[pair] = {"price": None}
    return forex

def fetch_crypto_quotes():
    quotes = {}
    try:
        tickers_obj = yf.Tickers(" ".join(CRYPTO_TICKERS))
        for sym in CRYPTO_TICKERS:
            try:
                info = tickers_obj.tickers[sym].info
                clean_sym = sym.replace('-USD', '')
                quotes[clean_sym] = {
                    "price": info.get("regularMarketPrice") or info.get("currentPrice"),
                    "change": info.get("regularMarketChange"),
                    "change_pct": info.get("regularMarketChangePercent"),
                    "name": info.get("shortName", sym)
                }
            except:
                quotes[sym.replace('-USD','')] = {"price": None, "change": None}
    except Exception as e:
        print(f"Crypto fetch error: {e}")
    return quotes

def fetch_stock_quotes():
    quotes = {}
    valid_tickers = [sym for sym in STOCK_TICKERS]
    try:
        tickers_obj = yf.Tickers(" ".join(valid_tickers))
        for sym in valid_tickers:
            try:
                info = tickers_obj.tickers[sym].info
                quotes[sym] = {
                    "price": info.get("regularMarketPrice") or info.get("currentPrice"),
                    "change": info.get("regularMarketChange"),
                    "change_pct": info.get("regularMarketChangePercent"),
                    "name": info.get("shortName", sym)
                }
            except:
                quotes[sym] = {"price": None, "change": None}
    except Exception as e:
        print(f"Stock fetch error: {e}")
    return quotes

# ─── GLOBAL STATE ─────────────────────────────────────────────────
state_lock = threading.Lock()
live_state = {}

def make_json_safe(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj

# ... (All utility functions: summarize_text, extract_countries, categorize_article, fetch_all_rss, generate_stock_history, fetch_nvd_vulns, generate_briefing, transform_to_threats, generate_company_list remain exactly as in original)

def refresh_all():
    global live_state
    with state_lock:
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Refreshing data...")
        articles = fetch_all_rss()
        stocks = fetch_stock_quotes()
        crypto = fetch_crypto_quotes()
        stock_history = generate_stock_history()
        threats = transform_to_threats(articles)
        vulns = fetch_nvd_vulns()
        companies = generate_company_list()
        forex = fetch_forex_rates()
        briefing = generate_briefing(articles)

        new_state = {
            "articles": articles,
            "stocks": stocks,
            "crypto": crypto,
            "stock_history": stock_history,
            "threats": threats,
            "vulns": vulns,
            "companies": companies,
            "briefings": [briefing],
            "ticker_news": [a["title"] for a in articles[:20]],
            "incidents": len(threats),
            "last_updated": datetime.utcnow().isoformat(),
            "sources_count": sum(len(v) for v in RSS_FEEDS.values()),
            "forex": forex
        }
        live_state = make_json_safe(new_state)
        
        # Real-time Crypto Price Alerts
        for sym, data in list(crypto.items())[:5]:
            if data.get("change_pct") and abs(data["change_pct"]) > 3:
                socketio.emit("crypto_alert", {
                    "symbol": sym,
                    "price": data["price"],
                    "change_pct": data["change_pct"],
                    "message": f"{sym} moved {data['change_pct']:+.1f}%"
                })
        
        # Threat Alert
        if threats:
            socketio.emit("alert", {
                "type": "threat",
                "title": "THREAT ALERT",
                "message": threats[0]["name"][:90],
                "severity": threats[0]["sev"]
            })
        
    socketio.emit("live_data", live_state)

def forex_updater():
    while True:
        time.sleep(60)
        try:
            forex = fetch_forex_rates()
            with state_lock:
                if "forex" in live_state:
                    live_state["forex"] = forex
            socketio.emit("forex_update", {"forex": forex})
        except:
            pass

def stock_updater():
    while True:
        time.sleep(30)
        try:
            stocks = fetch_stock_quotes()
            with state_lock:
                live_state["stocks"] = stocks
            socketio.emit("stock_update", {"stocks": stocks})
        except:
            pass

def crypto_updater():
    while True:
        time.sleep(45)
        try:
            crypto = fetch_crypto_quotes()
            with state_lock:
                live_state["crypto"] = crypto
            socketio.emit("crypto_update", {"crypto": crypto})
        except:
            pass

# WebSocket Heartbeat
@socketio.on('ping')
def handle_ping():
    emit('pong')

# Routes
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/api/state")
def full_state():
    return jsonify(live_state)

@socketio.on("connect")
def on_connect(auth=None):
    emit("live_data", live_state)

@socketio.on("request_refresh")
def on_refresh(auth=None):
    refresh_all()

if __name__ == "__main__":
    print("🚀 NEXON PULSE v2.3 — Crypto Alerts + Heartbeat")
    refresh_all()
    threading.Thread(target=stock_updater, daemon=True).start()
    threading.Thread(target=forex_updater, daemon=True).start()
    threading.Thread(target=crypto_updater, daemon=True).start()
    def scheduled():
        while True:
            time.sleep(300)
            refresh_all()
    threading.Thread(target=scheduled, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
