import os, io, re, json
from flask import Flask, request, jsonify
from pdfminer.high_level import extract_text
from datetime import date

# ----------------------
# Config
# ----------------------
APP_SECRET = os.getenv("INGEST_SECRET", "REPLACE_WITH_STRONG_RANDOM_SECRET")
RULES_PATH = os.getenv("RULES_PATH", "rules.json")

app = Flask(__name__)

# (Optional but handy): allow browser tools to call the API
try:
    from flask_cors import CORS  # add flask-cors to requirements.txt
    CORS(app)
except Exception:
    pass

# ----------------------
# Rules store helpers
# ----------------------
def load_rules():
    if os.path.exists(RULES_PATH):
        with open(RULES_PATH, "r") as f:
            return json.load(f)
    return {}

def save_rules(rules):
    with open(RULES_PATH, "w") as f:
        json.dump(rules, f, indent=2, sort_keys=True)

def _money(s: str) -> float:
    try:
        return float(s.replace(",", ""))
    except Exception:
        return 0.0

# ----------------------
# Robust ticket parser (PX/CG)
# ----------------------
def parse_ticket_pdf(pdf_bytes: bytes) -> dict:
    """
    Robust parser for PX/CG tickets:
    - carrier, currency
    - route from FROM/TO, ORIGIN/DESTINATION, normal pairs, or Fare Calculation line
    - taxes: supports CODE->amount and amount->CODE (e.g., 'PGK 22.80GC')
    - base & total
    """
    raw = extract_text(io.BytesIO(pdf_bytes)) or ""
    T = raw.upper()
    lines = [re.sub(r'\s+', ' ', L).strip() for L in T.splitlines() if L.strip()]

    # carrier
    carrier = "UNK"
    if " AIR NIUGINI" in T or re.search(r'\bPX\b', T): carrier = "PX"
    if " PNG AIR" in T or re.search(r'\bCG\b', T):      carrier = "CG"

    # currency
    currency = "PGK"
    mcur = re.search(r'\b(PGK)\b', T)
    if mcur: currency = mcur.group(1)

    # IATA sets
    BAD = {"FOR","THE","PNG","AIR","PGK","TTL","TAX","TOTAL","AMOUNT","RECEIPT","END"}
    PNG = {"POM","LAE","HGU","RAB","GUR","WWK","HKN","MAG","KVG","PNP","KRI","GKA","KMA","LNV","BUA","KIE","MDU"}
    REG = {"BNE","CNS","TSV","HKG","SIN","SYD","BKK","MNL","MEL"}
    IATA = PNG | REG

    # ---- route detection ----
    route = "UNK-UNK"

    # A) FROM XXX TO YYY
    m = re.search(r'\bFROM\s+([A-Z]{3})\b.{0,20}\bTO\s+([A-Z]{3})\b', T)
    if m and m.group(1) in IATA and m.group(2) in IATA:
        route = f"{m.group(1)}-{m.group(2)}"
    else:
        # B) ORIGIN / DEST(INATION)
        m2a = re.search(r'\bORIGIN\b[:\s]+([A-Z]{3})', T)
        m2b = re.search(r'\bDEST(?:INATION)?\b[:\s]+([A-Z]{3})', T)
        if m2a and m2b and m2a.group(1) in IATA and m2b.group(1) in IATA:
            route = f"{m2a.group(1)}-{m2b.group(1)}"
        else:
            # C) Normal pairs: XXX-YYY | XXX/YYY | XXX YYY
            pairs = re.findall(r'\b([A-Z]{3})\s*[-/ ]\s*([A-Z]{3})\b', T)
            got = False
            for a, b in pairs:
                if a not in BAD and b not in BAD and a in IATA and b in IATA:
                    route = f"{a}-{b}"
                    got = True
                    break

            # D) Fare calculation fallback (handles 'MAG CG WWK238.00PGK...')
            if not got:
                raw_up = raw.upper()
                mfc = re.search(r'FARE\s+CALCULATION.*?\n([^\n]{0,200})', raw_up)
                fc_line = mfc.group(1) if mfc else ""
                if not fc_line:
                    mfc2 = re.search(r'^\s*:\s*([^\n]*END[^\n]*)$', raw_up, re.M)
                    fc_line = mfc2.group(1) if mfc2 else ""
                tokens = re.findall(r'(?<![A-Z])[A-Z]{3}(?![A-Z])', fc_line)
                airports = [t for t in tokens if t in IATA and t not in BAD and t != "PGK"]
                if len(airports) >= 2:
                    route = f"{airports[0]}-{airports[1]}"

    # ---- taxes ----
    codes = ("YQ","YR","XT","GC","I9")
    components = {c: 0.0 for c in codes}

    # 1) compact tax line
    tax_line = next((L for L in lines if "TAX" in L), None)
    if tax_line:
        for c in codes:
            m = re.search(rf'\b{c}\b[:\s]+([0-9]{{1,3}}(?:,[0-9]{{3}})*\.[0-9]{{2}})', tax_line)
            if m:
                components[c] = max(components[c], float(m.group(1).replace(',', '')))
            m2 = re.search(rf'(?:PGK\s*)?([0-9]{{1,3}}(?:,[0-9]{{3}})*\.[0-9]{{2}})\s*{c}\b', tax_line)
            if m2:
                components[c] = max(components[c], float(m2.group(1).replace(',', '')))

    # 2) spread across lines
    for i, L in enumerate(lines):
        for c in codes:
            if components[c] > 0:
                continue
            if re.search(rf'\b{c}\b', L):
                m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
                if m:
                    components[c] = float(m.group(1).replace(',', ''))
                elif i + 1 < len(lines):
                    m2 = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', lines[i+1])
                    if m2:
                        components[c] = float(m2.group(1).replace(',', ''))
            else:
                m3 = re.search(rf'(?:PGK\s*)?([0-9]{{1,3}}(?:,[0-9]{{3}})*\.[0-9]{{2}})\s*{c}\b', L)
                if m3:
                    components[c] = max(components[c], float(m3.group(1).replace(',', '')))

    # Map UN/NX into XT (so totals match Selling Platform)
    for alias in ("UN","NX"):
        m = re.search(rf'(?:PGK\s*)?([0-9]{{1,3}}(?:,[0-9]{{3}})*\.[0-9]{{2}})\s*{alias}\b', T)
        if m:
            components["XT"] = round(components["XT"] + float(m.group(1).replace(',', '')), 2)

    # base & total
    base = 0.0
    for L in lines:
        if "BASE FARE" in L or re.search(r'\b(AIR\s*FARE|FARE)\b', L):
            m = re.search(r'(?:PGK\s*)?([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m:
                base = max(base, float(m.group(1).replace(',', '')))

    total = 0.0
    for L in lines:
        if re.search(r'\b(GRAND\s+TOTAL|TOTAL\s+AMOUNT|TOTAL\s+FARE|TOTAL)\b', L):
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', L)
            if m:
                total = max(total, float(m.group(1).replace(',', '')))
    if total == 0.0:
        amts = [float(x.replace(',', '')) for x in re.findall(r'([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})', T)]
        total = max(amts) if amts else 0.0

    taxes_sum = sum(components.values())
    if base == 0.0 and total > taxes_sum > 0:
        base = round(total - taxes_sum, 2)

    return {
        "carrier": carrier,
        "route": route,
        "currency": currency,
        "components": {"base": base, **components},
        "total": total
    }

# ----------------------
# Upsert rule from parsed ticket
# ----------------------
def upsert_rule(ticket: dict, pos: str = "PG"):
    key = f'{ticket["carrier"]}|{ticket["route"]}|{pos}|{ticket["currency"]}'
    rules = load_rules()
    r = rules.get(key, {})

    yqyr = float(ticket["components"].get("YQ", 0)) + float(ticket["components"].get("YR", 0))
    xt   = float(ticket["components"].get("XT", 0))
    gc   = float(ticket["components"].get("GC", 0))
    i9   = float(ticket["components"].get("I9", 0))

    if yqyr: r["yqyr_offset"] = round(yqyr, 2)
    if xt:   r["xt_offset"]   = round(xt, 2)
    if gc:   r["gc_tax"]      = round(gc, 2)
    if i9:   r["i9_tax"]      = round(i9, 2)

    r["last_verified_at"] = date.today().isoformat()
    rules[key] = r
    save_rules(rules)
    return key, r

# ----------------------
# Fare patching helpers
# ----------------------
def find_rule(carrier: str, origin: str, dest: str, currency: str = "PGK", pos: str = "PG"):
    rules = load_rules()
    route = f"{origin.upper()}-{dest.upper()}"
    key = f"{carrier.upper()}|{route}|{pos.upper()}|{currency.upper()}"
    if key in rules:
        return key, rules[key]
    rev_key = f"{carrier.upper()}|{dest.upper()}-{origin.upper()}|{pos.upper()}|{currency.upper()}"
    if rev_key in rules:
        return rev_key, rules[rev_key]
    return None, {}

def patch_fare(base_fare: float, carrier: str, origin: str, dest: str,
               currency: str = "PGK", pos: str = "PG", markup_pct: float = 8.8):
    key, rule = find_rule(carrier, origin, dest, currency, pos)
    yqyr = float(rule.get("yqyr_offset", 0))
    xt   = float(rule.get("xt_offset", 0))
    gc   = float(rule.get("gc_tax", 0))
    i9   = float(rule.get("i9_tax", 0))

    sp_total = round(float(base_fare) + yqyr + xt + gc + i9, 2)
    final_total = round(sp_total * (1 + markup_pct/100.0), 2)

    return {
        "rule_key": key,  # None if no match
        "components": {
            "base": round(float(base_fare), 2),
            "yqyr": yqyr, "xt": xt, "gc": gc, "i9": i9
        },
        "selling_platform_total": sp_total,
        "markup_pct": markup_pct,
        "final_total": final_total,
        "currency": currency.upper(),
        "pos": pos.upper(),
        "carrier": carrier.upper(),
        "route": f"{origin.upper()}-{dest.upper()}"
    }

# ----------------------
# Endpoints
# ----------------------
@app.post("/ingest-ticket")
def ingest_ticket():
    if (request.form.get("secret") or request.headers.get("X-Secret")) != APP_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "no file"}), 400
    pdf_bytes = f.read()
    ticket = parse_ticket_pdf(pdf_bytes)
    key, rule = upsert_rule(ticket)
    return jsonify({"ok": True, "rule_key": key, "parsed": ticket, "updated_rule": rule})

@app.get("/_test_upload")
def test_form():
    return f"""
      <form action="/ingest-ticket" method="post" enctype="multipart/form-data">
        <input type="hidden" name="secret" value="{APP_SECRET}">
        <input type="file" name="file" accept="application/pdf">
        <button type="submit">Upload PDF</button>
      </form>
    """

@app.route("/_dump_text", methods=["GET","POST"])
def dump_text():
    if request.method == "GET":
        return f"""
          <form action="/_dump_text" method="post" enctype="multipart/form-data">
            <input type="hidden" name="secret" value="{APP_SECRET}">
            <input type="file" name="file" accept="application/pdf">
            <button type="submit">Dump PDF Text</button>
          </form>
        """
    if (request.form.get("secret") or request.headers.get("X-Secret")) != APP_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "no file"}), 400
    txt = extract_text(io.BytesIO(f.read())) or ""
    return jsonify({"text": txt[:10000]})

@app.get("/rules")
def view_rules():
    return jsonify(load_rules())

@app.post("/quote")
def quote():
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"error": "invalid_json"}), 400

    for k in ("carrier","origin","dest","base_fare"):
        if k not in data:
            return jsonify({"error": f"missing_field:{k}"}), 400

    res = patch_fare(
        base_fare=data["base_fare"],
        carrier=data["carrier"],
        origin=data["origin"],
        dest=data["dest"],
        currency=data.get("currency","PGK"),
        pos=data.get("pos","PG"),
        markup_pct=float(data.get("markup_pct", 8.8))
    )

    # Debug to Render logs (helps when rule_key is None)
    print("QUOTE lookup:", res.get("rule_key"), "all_keys:", list(load_rules().keys()))
    return jsonify({"ok": True, **res})

@app.get("/test_quote")
def test_quote_form():
    return """
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Test /quote</title></head>
  <body style="font-family:system-ui;max-width:720px;margin:2rem auto;">
    <h2>Test /quote</h2>
    <form id="f">
      <label>Carrier <input name="carrier" value="CG" required></label><br><br>
      <label>Origin  <input name="origin"  value="MAG" required></label><br><br>
      <label>Dest    <input name="dest"    value="WWK" required></label><br><br>
      <label>Base fare (PGK) <input name="base_fare" type="number" step="0.01" value="238.00" required></label><br><br>
      <label>Currency <input name="currency" value="PGK"></label><br><br>
      <label>POS <input name="pos" value="PG"></label><br><br>
      <label>Markup % <input name="markup_pct" type="number" step="0.1" value="8.8"></label><br><br>
      <button type="submit">Send</button>
    </form>
    <pre id="out" style="white-space:pre-wrap;background:#f6f6f6;padding:1rem;border:1px solid #ddd;margin-top:1rem;"></pre>
    <script>
      const f = document.getElementById('f');
      const out = document.getElementById('out');
      f.addEventListener('submit', async (e) => {
        e.preventDefault();
        const data = Object.fromEntries(new FormData(f).entries());
        data.base_fare = parseFloat(data.base_fare);
        data.markup_pct = parseFloat(data.markup_pct);
        out.textContent = "Sending...";
        const res = await fetch('/quote', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(data)
        });
        out.textContent = await res.text();
      });
    </script>
  </body>
</html>
"""

@app.get("/")
def health():
    return jsonify({"ok": True, "message": "Elite Ticket Ingest API is running."})
