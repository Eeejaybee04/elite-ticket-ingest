import os, io, re, json
from flask import Flask, request, jsonify
from pdfminer.high_level import extract_text
from datetime import date

# Secret for authentication between Gmail script and this API
APP_SECRET = os.getenv("INGEST_SECRET", "REPLACE_WITH_STRONG_RANDOM_SECRET")
RULES_PATH = os.getenv("RULES_PATH", "rules.json")

app = Flask(__name__)

# Tax codes we want to track for PX and CG tickets
CODES = ("YQ", "YR", "GC", "I9", "XT")

def load_rules():
    """Load existing rules.json or return empty dict"""
    if os.path.exists(RULES_PATH):
        with open(RULES_PATH, "r") as f:
            return json.load(f)
    return {}

def save_rules(rules):
    """Save updated rules back to rules.json"""
    with open(RULES_PATH, "w") as f:
        json.dump(rules, f, indent=2, sort_keys=True)

def _money(s):
    try:
        return float(s.replace(",", ""))
    except Exception:
        return 0.0

def parse_ticket_pdf(pdf_bytes):
    """
    Parse a BSP or e-ticket PDF for PX / CG.
    Extract base fare, taxes, YQ/YR, XT, GC, I9, total amount.
    """
    text = extract_text(io.BytesIO(pdf_bytes)) or ""
    lines = [re.sub(r'\s+', ' ', L).strip() for L in text.splitlines() if L.strip()]

    # Carrier detection: PX or CG
    carrier = "UNK"
    if re.search(r'\b(PX)\b', text, re.I): carrier = "PX"
    if re.search(r'\b(CG)\b', text, re.I): carrier = "CG"
    if "Air Niugini" in text: carrier = "PX"
    if "PNG AIR" in text.upper(): carrier = "CG"

    # Default currency PGK; detect if explicitly present
    currency = "PGK"
    mcur = re.search(r'\b(PGK)\b', text)
    if mcur: currency = mcur.group(1)

    # Route detection (POM-LAE, LAE-HKN, etc.)
    route = "UNK-UNK"
    mroute = re.search(r'\b([A-Z]{3})[-\s/]([A-Z]{3})\b', text)
    if mroute:
        route = f"{mroute.group(1)}-{mroute.group(2)}"

    # Extract tax components line by line
    components = {c: 0.0 for c in CODES}
    for i, L in enumerate(lines):
        for c in CODES:
            if re.search(rf'\b{c}\b', L):
                # Try to get amount on the same line
                m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
                if m:
                    components[c] = max(components[c], _money(m.group(1)))
                # Check next line (some PDFs split values)
                if i + 1 < len(lines):
                    m2 = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', lines[i+1])
                    if m2:
                        components[c] = max(components[c], _money(m2.group(1)))

    # Base fare & total detection
    base = 0.0
    total = 0.0
    for L in lines:
        if re.search(r'\b(BASE\s*FARE|FARE)\b', L, re.I):
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m: base = max(base, _money(m.group(1)))
        if re.search(r'\b(TOTAL|TOTAL AMOUNT|TOTAL FARE|GRAND TOTAL)\b', L, re.I):
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m: total = max(total, _money(m.group(1)))

    # Fallback for total: pick highest amount found anywhere
    if total == 0.0:
        amts = [_money(m) for m in re.findall(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', text)]
        total = max(amts) if amts else 0.0

    return {
        "carrier": carrier,
        "route": route,
        "currency": currency,
        "components": {"base": base, **components},
        "total": total
    }

def upsert_rule(ticket, pos="PG"):
    """
    Use parsed ticket data to update/add a rule in rules.json.
    Key = carrier|route|pos|currency.
    """
    key = f'{ticket["carrier"]}|{ticket["route"]}|{pos}|{ticket["currency"]}'
    rules = load_rules()
    r = rules.get(key, {})

    yqyr = float(ticket["components"].get("YQ", 0)) + float(ticket["components"].get("YR", 0))
    xt    = float(ticket["components"].get("XT", 0))
    gc    = float(ticket["components"].get("GC", 0))
    i9    = float(ticket["components"].get("I9", 0))

    # Update only if we have valid data
    if yqyr: r["yqyr_offset"] = round(yqyr, 2)
    if xt:   r["xt_offset"]   = round(xt, 2)
    if gc:   r["gc_tax"]      = round(gc, 2)
    if i9:   r["i9_tax"]      = round(i9, 2)

    r["last_verified_at"] = date.today().isoformat()
    rules[key] = r
    save_rules(rules)
    return key, r

@app.post("/ingest-ticket")
def ingest_ticket():
    """Endpoint Gmail Apps Script will POST PDFs to"""
    if (request.form.get("secret") or request.headers.get("X-Secret")) != APP_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "no file"}), 400
    pdf_bytes = f.read()
    ticket = parse_ticket_pdf(pdf_bytes)
    key, rule = upsert_rule(ticket)
    return jsonify({"ok": True, "rule_key": key, "parsed": ticket, "updated_rule": rule})

@app.get("/")
def health():
    return jsonify({"ok": True, "message": "Elite Ticket Ingest API is running."})
