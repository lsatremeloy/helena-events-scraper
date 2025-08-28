# pip install -r requirements.txt
# requirements: ics requests python-dateutil pytz feedparser playwright extruct w3lib

import os, csv, json, hashlib, datetime as dt, pytz, requests, feedparser, sys, re
from dateutil import parser as dp
from ics import Calendar
from playwright.sync_api import sync_playwright
import extruct
from w3lib.html import get_base_url
from urllib.parse import urljoin

# --- Config -------------------------------------------------------------------
AIRTABLE_WEBHOOK = os.environ.get("AIRTABLE_WEBHOOK")  # set via GitHub Secret
TZ = pytz.timezone("America/Denver")

# --- Utilities ----------------------------------------------------------------
def now_iso():
    return dt.datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()

def iso_date(d):
    if not d: return None
    try: return dp.parse(str(d)).astimezone(TZ).date().isoformat()
    except Exception: return None

def iso_time(d):
    if not d: return None
    try: return dp.parse(str(d)).astimezone(TZ).strftime("%-I:%M %p")
    except Exception: return None

def weekday_name(d):
    try: return dp.parse(str(d)).date().strftime("%A")
    except Exception: return None

def sha1_id(*parts):
    return hashlib.sha1("||".join([p or "" for p in parts]).encode()).hexdigest()

def post_event(data):
    """POST one event to Airtable webhook using your generic schema."""
    payload = {"data": data}
    try:
        r = requests.post(AIRTABLE_WEBHOOK, json=payload, timeout=30)
        print(f"  → POST {data.get('event_name')!r} → {r.status_code}")
        r.raise_for_status()
    except Exception as e:
        print("  ! POST failed:", e)

# --- Extract schema.org Events from JSON-LD, Microdata, RDFa -------------------
def extract_structured_events(url, html):
    """Return schema.org Event dicts from JSON-LD, Microdata, and RDFa."""
    all_events = []

    data = extruct.extract(
        html,
        base_url=get_base_url(html, url),
        syntaxes=['json-ld', 'microdata', 'rdfa']
    )

    # ---------- JSON-LD ----------
    def as_list(x): return x if isinstance(x, list) else [x] if x else []
    for block in (data.get('json-ld') or []):
        types = as_list(block.get("@type"))
        if "Event" in types:
            all_events.append(block)
        elif block.get("@type") == "ItemList":
            for item in block.get("itemListElement", []) or []:
                ent = item.get("item") or {}
                if isinstance(ent, dict) and ("Event" in as_list(ent.get("@type"))):
                    all_events.append(ent)

    # ---------- Microdata ----------
    for md in (data.get('microdata') or []):
        types = md.get('type') or []
        if any("schema.org/Event" in t for t in types):
            props = md.get('properties') or {}
            all_events.append({
                "@type": "Event",
                "name": props.get("name"),
                "startDate": props.get("startDate"),
                "location": props.get("location"),
                "description": props.get("description"),
                "url": props.get("url"),
                "identifier": props.get("identifier"),
                "@id": props.get("id") or props.get("@id"),
            })

    # ---------- RDFa ----------
    for rd in (data.get('rdfa') or []):
        types = rd.get('type') or []
        if any("schema.org/Event" in t for t in types):
            props = rd.get('properties') or {}
            all_events.append({
                "@type": "Event",
                "name": props.get("name"),
                "startDate": props.get("startDate"),
                "location": props.get("location"),
                "description": props.get("description"),
                "url": props.get("url"),
                "identifier": props.get("identifier"),
                "@id": props.get("id") or props.get("@id"),
            })

    return all_events

# --- ICS ----------------------------------------------------------------------
def ingest_ics(url, source_name, default_location=None):
    print(f"Fetching ICS: {url}")
    try:
        resp = requests.get(url, timeout=30)
    except Exception as e:
        print("  ! Network error:", e); return

    ctype = (resp.headers.get("Content-Type") or "").lower()
    text = resp.text

    # Guard: some “ics” endpoints return HTML error pages
    if "text/calendar" not in ctype and "<html" in text[:200].lower():
        print("  ! Skipping ICS (returned HTML, not .ics)")
        return

    try:
        cal = Calendar(text)
    except Exception as e:
        print("  ! Bad ICS content, skipping:", e)
        return

    count = 0
    for e in cal.events:
        start = e.begin.datetime if e.begin else None
        data = {
            "event_name": (e.name or "").strip(),
            "date": iso_date(start),
            "day": weekday_name(start),
            "time": iso_time(start),
            "host_org": source_name,
            "description": (getattr(e, "description", "") or "").strip() or None,
            "cost": None,
            "tags": [],
            "location": default_location or getattr(e, "location", None),
            "address": None,
            "link": getattr(e, "url", None) or url,
            "status": "active",
            "source_id": getattr(e, "uid", None) or f"ics:{sha1_id(url, (e.name or '').strip(), iso_date(start))}",
            "last_seen_at": now_iso(),
        }
        print("  · Posting event:", data["event_name"] or "(untitled)")
        post_event(data); count += 1
    print(f"ICS done: posted {count} events")

# --- Page (Playwright) --------------------------------------------------------
def ingest_page(url, source_name, default_location=None):
    print(f"Fetching PAGE: {url}")
    attempts = 2
    html = None
    for i in range(attempts):
        try:
            with sync_playwright() as p:
                b = p.chromium.launch(args=["--no-sandbox"])
                page = b.new_page(user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122 Safari/537.36"
                ))
                page.goto(url, timeout=90000)
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_selector("body", timeout=30000)
                html = page.content()
                b.close()
            break
        except Exception as e:
            print(f"  ! Attempt {i+1} failed: {e}")
            if i == attempts - 1:
                print("  ! Giving up on page.")
                return

    evs = extract_structured_events(url, html) if html else []
    print(f"  · Found {len(evs)} structured event blocks")

    posted = 0
    for ev in evs:
        start = ev.get("startDate")
        loc   = ev.get("location") if isinstance(ev.get("location"), dict) else {}
        addr  = loc.get("address") if isinstance(loc.get("address"), dict) else {}
        data = {
            "event_name": (ev.get("name") or "").strip(),
            "date": iso_date(start),
            "day": weekday_name(start),
            "time": iso_time(start),
            "host_org": source_name,
            "description": (ev.get("description") or "").strip() or None,
            "cost": None,
            "tags": [],
            "location": (loc.get("name") if isinstance(loc.get("name"), str) else None) or default_location,
            "address": ", ".join(filter(None, [
                (addr.get("streetAddress") if isinstance(addr.get("streetAddress"), str) else None),
                (addr.get("addressLocality") if isinstance(addr.get("addressLocality"), str) else None),
                (addr.get("addressRegion") if isinstance(addr.get("addressRegion"), str) else None),
            ])).strip() or None,
            "link": ev.get("url") or url,
            "status": "active",
            "source_id": ev.get("@id") or ev.get("identifier") or f"jsonld:{sha1_id(url, (ev.get('name') or '').strip(), iso_date(start))}",
            "last_seen_at": now_iso(),
        }
        print("  · Posting event:", data["event_name"] or "(untitled)")
        post_event(data); posted += 1

    print(f"PAGE done: posted {posted} events")

# --- RSS ----------------------------------------------------------------------
def ingest_rss(url, source_name, default_location=None):
    print(f"Fetching RSS: {url}")
    feed = feedparser.parse(url)
    posted = 0
    for entry in feed.entries:
        start = entry.get("published") or entry.get("updated")
        data = {
            "event_name": entry.get("title"),
            "date": iso_date(start),
            "day": weekday_name(start),
            "time": iso_time(start),
            "host_org": source_name,
            "description": (entry.get("summary") or "")[:1000] or None,
            "cost": None,
            "tags": [t.get("term") for t in (entry.get("tags") or []) if t.get("term")] or [],
            "location": default_location,
            "address": None,
            "link": entry.get("link"),
            "status": "active",
            "source_id": f"rss:{sha1_id(url, entry.get('id') or entry.get('link') or entry.get('title'))}",
            "last_seen_at": now_iso(),
        }
        print("  · Posting event:", data["event_name"] or "(untitled)")
        post_event(data); posted += 1
    print(f"RSS done: posted {posted} events")

# --- Runner -------------------------------------------------------------------
def run_from_csv(path="sources.csv"):
    print("Reading sources from:", path)
    try:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                t = (row.get("type") or "").strip().lower()
                url = (row.get("url") or "").strip()
                name = (row.get("source_name") or "Unknown").strip()
                default_loc = (row.get("default_location") or "").strip()

                if not url or not t:
                    continue

                print(f"\n=== Processing row ===\n type={t}\n url={url}\n source_name={name}\n default_location={default_loc or '—'}")
                try:
                    if t == "ics":
                        ingest_ics(url, name, default_loc)
                    elif t == "rss":
                        ingest_rss(url, name, default_loc)
                    elif t in ("page", "jsonld"):
                        ingest_page(url, name, default_loc)
                    else:
                        print("Unknown type, skipping:", t, url)
                except Exception as e:
                    print(f"!! Error on {t} {url} -> {e}")
    except FileNotFoundError:
        print("Could not find sources.csv in the working directory.")
        sys.exit(1)

if __name__ == "__main__":
    print("Starting ingest…")
    if not AIRTABLE_WEBHOOK:
        print("ERROR: AIRTABLE_WEBHOOK is not set in env.")
        sys.exit(1)
    run_from_csv("sources.csv")
    print("\nAll done.")
