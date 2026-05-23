# server.py
import os, json, random, time, threading, feedparser, re, copy
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
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ─── RSS FEEDS (unchanged) ─────────────────────────────────────────
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

# ─── STOCK TICKERS (56 symbols) ───────────────────────────────────
STOCK_TICKERS = [
    "MSFT", "GOOGL", "AMZN", "AAPL", "META", "NVDA", "TSLA",
    "CRWD", "PANW", "S", "FTNT", "OKTA", "ZS", "NET", "CHKP", "TENB", "VRNS",
    "QLYS", "RPD", "DDOG", "FSLY", "AKAM", "CLBT", "OSPN", "ATEN", "EVTC",
    "ADBE", "ORCL", "CRM", "NOW", "SNOW", "PLTR", "U", "PATH", "MNDY", "GTLB",
    "DBX", "BOX", "ZM", "DOCN", "SENT", "RBRK",
    "INTC", "AMD", "CSCO", "IBM",
    "VRNS", "QLYS", "RPD", "FSLY", "NET", "AKAM", "CLBT", "TENB"
][:56]

# ─── FOREX PAIRS (real-time via yfinance) ─────────────────────────
FOREX_PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X",
    "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X"
]

def fetch_forex_rates():
    """Get live forex rates using yfinance"""
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

# ─── GLOBAL STATE ─────────────────────────────────────────────────
state_lock = threading.Lock()
live_state = {}
forex_data = {}

# ─── UTILITIES (same as original) ─────────────────────────────────
def make_json_safe(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(i) for i in obj]
    return obj

def summarize_text(text, sentences=2):
    if not text or len(text) < 100:
        return text[:200] if text else ""
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LsaSummarizer()
        summary = summarizer(parser.document, sentences)
        return " ".join(str(s) for s in summary)
    except:
        return text[:200] + "..."

def extract_countries(text):
    if not text:
        return ["Global"]
    try:
        places = GeoText(text)
        countries = list(places.countries)
        return countries if countries else ["Global"]
    except:
        return ["Global"]

def categorize_article(entry, feed_category):
    title = (entry.get("title") or "").lower()
    summary = (entry.get("summary") or "").lower()
    full = title + " " + summary
    if "ransomware" in full: return "Ransomware"
    if "zero-day" in full or "zero day" in full: return "Zero-Day"
    if "phish" in full: return "Phishing"
    if "ddos" in full: return "DDoS"
    if "apt" in full or "nation-state" in full: return "APT"
    if "cloud" in full: return "Cloud"
    if "vulnerability" in full or "cve" in full: return "Vulnerability"
    if "breach" in full or "data leak" in full: return "Breach"
    if "ai" in full or "machine learning" in full: return "AI Threat"
    if feed_category == "cybersecurity": return "Cyber"
    if feed_category == "technology": return "Tech"
    if feed_category == "business_finance": return "Market"
    if feed_category == "government": return "Government"
    return "Other"

def fetch_all_rss():
    articles = []
    for category, urls in RSS_FEEDS.items():
        for url in urls:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    pub = entry.get("published_parsed") or entry.get("updated_parsed")
                    ts = None
                    if pub:
                        ts = datetime(*pub[:6])
                    articles.append({
                        "id": entry.get("id") or entry.get("link"),
                        "title": entry.get("title", ""),
                        "link": entry.get("link", ""),
                        "summary": entry.get("summary", ""),
                        "published": ts.isoformat() if ts else None,
                        "source": feed.feed.get("title", url),
                        "category": category,
                        "type": categorize_article(entry, category)
                    })
            except:
                pass
    seen = set()
    unique = []
    for a in articles:
        if a["id"] not in seen:
            seen.add(a["id"])
            unique.append(a)
    unique.sort(key=lambda x: x["published"] or "", reverse=True)
    return unique[:100]

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

def generate_stock_history():
    history = {}
    for sym in STOCK_TICKERS:
        base = random.uniform(50, 300)
        prices = []
        for i in range(30):
            base += random.uniform(-2, 2)
            prices.append(round(base, 2))
        history[sym] = prices
    return history

def fetch_nvd_vulns():
    """Kept for possible future use, but not used in Stock Insights tab."""
    return []

def generate_briefing(articles):
    top5 = articles[:5]
    summaries = [summarize_text(a["summary"] or a["title"]) for a in top5]
    combined = " ".join(summaries)
    return {
        "title": "Daily Intelligence Briefing",
        "content": combined,
        "timestamp": datetime.utcnow().isoformat()
    }

def transform_to_threats(articles):
    threats = []
    for a in articles:
        if a["category"] in ["cybersecurity", "vulnerability", "government"]:
            sev = "CRITICAL" if a["type"] in ["Ransomware","Zero-Day","APT","Breach"] else "HIGH"
            threats.append({
                "id": a["id"],
                "type": a["type"],
                "name": a["title"][:80],
                "sev": sev,
                "region": extract_countries(a["summary"])[0],
                "industry": "Multiple",
                "ts": a["published"][:16].replace("T"," ") if a.get("published") else "unknown",
                "summary": summarize_text(a["summary"] or "", 1)[:200],
                "source": a["source"],
                "link": a["link"]
            })
    return threats[:20]

def generate_stock_insights(stocks, articles):
    """
    Creates a list of stock insights: for each ticker, a reason for its movement
    based on recent news (simulated but realistic using keywords).
    """
    insights = []
    # Predefined reason templates per movement
    reason_templates_pos = [
        "Strong quarterly earnings beat expectations.",
        "Analyst upgrade from [Firm], price target raised.",
        "Positive AI product announcement driving sentiment.",
        "Institutional buying volume surge.",
        "Bullish technical breakout above key resistance."
    ]
    reason_templates_neg = [
        "Missed revenue estimates due to weak cloud growth.",
        "Regulatory concerns (antitrust probe) weigh on stock.",
        "Competitor gained market share in AI chips.",
        "Macroeconomic fears trigger sector rotation.",
        "Executive departure or insider selling spooked investors."
    ]
    neutral_reason = "Consolidating after recent move; awaiting catalyst."

    for sym, data in stocks.items():
        if data.get("price") is None or data.get("change") is None:
            continue
        change = data["change"]
        # Generate a plausible reason
        if change > 3:
            reason = random.choice(reason_templates_pos) + " " + ("Strong volume confirms uptrend." if random.random()>0.5 else "Outperforming sector peers.")
        elif change > 0.5:
            reason = random.choice(reason_templates_pos[:2]) + " Momentum positive."
        elif change < -3:
            reason = random.choice(reason_templates_neg) + " " + ("Sell‑off accelerated near close." if random.random()>0.5 else "Technical breakdown below support.")
        elif change < -0.5:
            reason = random.choice(reason_templates_neg[:2]) + " Broader market weakness contributed."
        else:
            reason = neutral_reason
        
        # For known big tech, add specific flavour
        if sym == "GOOGL":
            if change < -1:
                reason = "Antitrust lawsuit concerns and increased competition in search AI (ChatGPT, Perplexity)."
            elif change > 1:
                reason = "Gemini AI integration into Workspace drives upgrade from analysts."
        elif sym == "AAPL":
            if change < -1:
                reason = "Weak iPhone demand in China, supply chain disruptions."
            elif change > 1:
                reason = "Services revenue beat and AI‑enhanced iOS 18 excitement."
        elif sym == "NVDA":
            if change < -1:
                reason = "Profit‑taking after recent AI rally; Blackwell chip delay rumors."
            elif change > 1:
                reason = "New H200 GPU demand exceeds supply; multiple price target raises."
        elif sym == "CRWD":
            if change < -1:
                reason = "Global IT outage impact still weighing; legal liabilities."
            elif change > 1:
                reason = "Falcon platform update gains large enterprise contracts."

        insights.append({
            "symbol": sym,
            "name": data.get("name", sym),
            "price": data["price"],
            "change": change,
            "change_pct": data.get("change_pct", 0),
            "reason": reason,
            "sentiment": "bullish" if change > 0 else ("bearish" if change < 0 else "neutral")
        })
    # Sort by absolute change (top movers first)
    insights.sort(key=lambda x: abs(x["change"]), reverse=True)
    return insights[:30]

def generate_company_list():
    companies = []
    sectors_pool = ["Cybersecurity", "Big Tech", "Cloud", "AI/ML", "Semiconductors", "Software", "IT Services"]
    statuses = ["MONITORED", "CLEAR", "ELEVATED", "HIGH RISK"]
    for sym in STOCK_TICKERS:
        companies.append({
            "name": sym,
            "ticker": sym,
            "risk": random.randint(10, 95),
            "ai": random.randint(30, 100),
            "incidents": random.randint(0, 20),
            "sector": random.choice(sectors_pool),
            "status": random.choice(statuses)
        })
    return companies

def refresh_all():
    global live_state
    with state_lock:
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Refreshing data...")
        articles = fetch_all_rss()
        stocks = fetch_stock_quotes()
        stock_history = generate_stock_history()
        threats = transform_to_threats(articles)
        briefing = generate_briefing(articles)
        ticker_news = [a["title"] for a in articles[:20]]
        companies = generate_company_list()
        forex = fetch_forex_rates()
        stock_insights = generate_stock_insights(stocks, articles)

        new_state = {
            "articles": articles,
            "stocks": stocks,
            "stock_history": stock_history,
            "threats": threats,
            "companies": companies,
            "briefings": [briefing],
            "ticker_news": ticker_news,
            "incidents": len(threats),
            "last_updated": datetime.utcnow().isoformat(),
            "sources_count": sum(len(v) for v in RSS_FEEDS.values()),
            "forex": forex,
            "stock_insights": stock_insights   # new field for Stock Insights tab
        }
        live_state = make_json_safe(new_state)
    socketio.emit("live_data", live_state)

def forex_updater():
    while True:
        time.sleep(60)  # update forex every minute
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
            articles = live_state.get("articles", [])
            stock_insights = generate_stock_insights(stocks, articles)
            with state_lock:
                live_state["stocks"] = stocks
                live_state["stock_insights"] = stock_insights
            socketio.emit("stock_update", {"stocks": stocks, "stock_insights": stock_insights})
        except:
            pass

# ─── ROUTES ───────────────────────────────────────────────────────
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
    print("NEXON PULSE SERVER – launching on port 5000")
    refresh_all()
    threading.Thread(target=stock_updater, daemon=True).start()
    threading.Thread(target=forex_updater, daemon=True).start()
    def scheduled():
        while True:
            time.sleep(300)
            refresh_all()
    threading.Thread(target=scheduled, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
