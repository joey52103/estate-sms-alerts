import csv
import os
import re
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict

from dotenv import load_dotenv
from flask import Flask, request, redirect, url_for, session, render_template_string
import phonenumbers
from twilio.twiml.messaging_response import MessagingResponse

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "CHANGE_ME_PLEASE")

DEFAULT_REGION = os.getenv("DEFAULT_REGION", "US")
DB_PATH = os.getenv("DB_PATH", "contacts.db")
EXPORT_CSV = os.getenv("CONTACTS_CSV", "contacts.csv")
OPTOUT_FILE = os.getenv("OPTOUT_FILE", "optouts.txt")

ADMIN_USER = os.getenv("ADMIN_USER", "dad")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

OPTIN_KEYWORDS = {"JOIN", "START", "SUBSCRIBE"}
OPTOUT_KEYWORDS = {"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT", "STOPA", "STOP1", "STOP2"}
HELP_KEYWORDS = {"HELP", "INFO"}

ASK_NAME_ON_JOIN = True


# -------------------------
# Helpers
# -------------------------
def utc_now() -> str:
    return datetime.utcnow().isoformat()


def normalize_e164(raw: str, default_region: str = "US") -> Optional[str]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        parsed = phonenumbers.parse(raw, None if raw.startswith("+") else default_region)
        if not phonenumbers.is_valid_number(parsed):
            return None
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None


def clean_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^a-zA-Z\s'\-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:40]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    # Matches YOUR current schema:
    # id INTEGER PK, phone TEXT NOT NULL, name TEXT, opted_out INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL UNIQUE,
                name TEXT DEFAULT '',
                opted_out INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def get_contact_by_phone(phone: str) -> Optional[dict]:
    init_db()
    with db() as conn:
        row = conn.execute(
            "SELECT id, phone, name, opted_out, created_at, updated_at FROM contacts WHERE phone = ?",
            (phone,),
        ).fetchone()
        return dict(row) if row else None


def add_contact(phone: str, name: str = "") -> bool:
    """Returns True if added, False if already exists."""
    init_db()
    now = utc_now()
    try:
        with db() as conn:
            conn.execute(
                """
                INSERT INTO contacts (phone, name, opted_out, created_at, updated_at)
                VALUES (?, ?, 0, ?, ?)
                """,
                (phone, name, now, now),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def set_opted_out(phone: str, opted_out: int) -> None:
    init_db()
    with db() as conn:
        conn.execute(
            "UPDATE contacts SET opted_out = ?, updated_at = ? WHERE phone = ?",
            (1 if opted_out else 0, utc_now(), phone),
        )


def delete_contact(phone: str) -> None:
    init_db()
    with db() as conn:
        conn.execute("DELETE FROM contacts WHERE phone = ?", (phone,))


def update_contact(old_phone: str, new_phone: str, new_name: str) -> Optional[str]:
    """
    Updates phone and name. Returns error string or None on success.
    """
    init_db()
    old = get_contact_by_phone(old_phone)
    if not old:
        return "Contact not found."

    # If phone changed to another existing phone -> error
    if new_phone != old_phone and get_contact_by_phone(new_phone):
        return "That phone number already exists."

    with db() as conn:
        if new_phone != old_phone:
            conn.execute(
                """
                UPDATE contacts
                SET phone = ?, name = ?, updated_at = ?
                WHERE phone = ?
                """,
                (new_phone, new_name, utc_now(), old_phone),
            )
        else:
            conn.execute(
                """
                UPDATE contacts
                SET name = ?, updated_at = ?
                WHERE phone = ?
                """,
                (new_name, utc_now(), old_phone),
            )
    return None


def list_contacts(q: str = "") -> List[Dict]:
    init_db()
    q = (q or "").strip()
    with db() as conn:
        if q:
            like = f"%{q}%"
            rows = conn.execute(
                """
                SELECT id, phone, name, opted_out, created_at, updated_at
                FROM contacts
                WHERE phone LIKE ? OR name LIKE ?
                ORDER BY updated_at DESC
                """,
                (like, like),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, phone, name, opted_out, created_at, updated_at
                FROM contacts
                ORDER BY updated_at DESC
                """
            ).fetchall()
    return [dict(r) for r in rows]


def export_contacts_csv_and_optouts() -> None:
    init_db()
    with db() as conn:
        opted_in = conn.execute(
            "SELECT phone, name FROM contacts WHERE opted_out = 0 ORDER BY updated_at DESC"
        ).fetchall()
        opted_out = conn.execute(
            "SELECT phone FROM contacts WHERE opted_out = 1 ORDER BY updated_at DESC"
        ).fetchall()

    with open(EXPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["phone", "name"])
        for row in opted_in:
            w.writerow([row["phone"], row["name"] or ""])

    with open(OPTOUT_FILE, "w", encoding="utf-8") as f:
        for row in opted_out:
            f.write(row["phone"] + "\n")


def tokens_upper(body: str) -> set:
    body = (body or "").strip().upper()
    return set(body.split())


def admin_logged_in() -> bool:
    return session.get("admin_authed") is True


def require_admin():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))
    return None


# -------------------------
# Admin UI
# -------------------------
BASE_HTML = """
<!doctype html>
<html>
<head>
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{font-family:system-ui;margin:40px;max-width:980px}
    .card{border:1px solid #ddd;border-radius:14px;padding:18px}
    input,select{padding:10px;margin:6px 0;border:1px solid #ccc;border-radius:10px;width:100%}
    button{padding:10px 14px;border:0;border-radius:10px;background:#0b5fff;color:white;cursor:pointer}
    .btn2{background:#555}
    .btnDanger{background:#b00020}
    .ok{color:#0a7a2f;font-weight:700}
    .err{color:#b00020;font-weight:700}
    a{color:#0b5fff;text-decoration:none}
    .nav{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;padding-bottom:12px;border-bottom:1px solid #eee}
    .navleft a{margin-right:14px;font-weight:600}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #ddd;padding:10px;text-align:left;vertical-align:top}
    th{background:#f7f7f7}
    .rowActions form{display:inline}
    .pill{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px}
    .in{background:#e9f7ef;color:#0a7a2f}
    .out{background:#fdecea;color:#b00020}
    .muted{color:#666}
    .searchRow{display:flex;gap:10px;align-items:center;margin:10px 0 18px}
    .searchRow input{flex:1}
  </style>
</head>
<body>
  {% if show_nav %}
  <div class="nav">
    <div class="navleft">
      <a href="/admin/add">Add Contact</a>
      <a href="/admin/contacts">Contacts</a>
    </div>
    <div class="navright">
      <a href="/admin/logout">Logout</a>
    </div>
  </div>
  {% endif %}

  {{ body|safe }}
</body>
</html>
"""


def render_admin(title: str, body: str, *, show_nav: bool = True) -> str:
    return render_template_string(BASE_HTML, title=title, body=body, show_nav=show_nav)


@app.route("/")
def home():
    return redirect(url_for("admin_login"))


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if admin_logged_in():
        return redirect(url_for("admin_add"))

    error = ""
    if request.method == "POST":
        user = (request.form.get("user") or "").strip()
        pw = (request.form.get("password") or "").strip()

        # IMPORTANT: ADMIN_PASSWORD must be set (non-empty) in .env
        if user == ADMIN_USER and ADMIN_PASSWORD and pw == ADMIN_PASSWORD:
            session["admin_authed"] = True
            return redirect(url_for("admin_add"))
        error = "Invalid login."

    body = f"""
    <h2>J Maslanka Estates – Admin</h2>
    <div class="card" style="max-width:520px">
      <form method="post">
        <label>Username</label>
        <input name="user" autocomplete="username" required />
        <label>Password</label>
        <input name="password" type="password" autocomplete="current-password" required />
        <button type="submit" style="width:100%">Sign in</button>
        {"<div class='err' style='margin-top:10px'>" + error + "</div>" if error else ""}
      </form>
    </div>
    """
    return render_admin("Admin Login", body, show_nav=False)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@app.route("/admin/add", methods=["GET", "POST"])
def admin_add():
    gate = require_admin()
    if gate:
        return gate

    ok = ""
    error = ""

    if request.method == "POST":
        raw_phone = request.form.get("phone") or ""
        raw_name = request.form.get("name") or ""

        phone = normalize_e164(raw_phone, DEFAULT_REGION)
        name = clean_name(raw_name)

        if not phone:
            error = "That phone number looks invalid. Try again (include area code)."
        else:
            added = add_contact(phone, name)
            if not added:
                error = "Number already added."
            else:
                export_contacts_csv_and_optouts()
                ok = f"Added: {phone}" + (f" ({name})" if name else "")

    body = f"""
    <h2>Admin – Add Contact</h2>
    <div class="card" style="max-width:720px">
      <form method="post">
        <label>Phone number</label>
        <input name="phone" placeholder="(412) 555-1234" required />
        <label>Name (optional)</label>
        <input name="name" placeholder="Joey" />
        <button type="submit">Add</button>
      </form>

      {"<p class='ok'>" + ok + "</p>" if ok else ""}
      {"<p class='err'>" + error + "</p>" if error else ""}

      <p class="muted"><small>Saved to database and exported to contacts.csv automatically.</small></p>
    </div>
    """
    return render_admin("Add Contact", body)


@app.route("/admin/contacts", methods=["GET"])
def admin_contacts():
    gate = require_admin()
    if gate:
        return gate

    q = request.args.get("q", "") or ""
    contacts = list_contacts(q=q)

    rows_html = ""
    for c in contacts:
        phone = c["phone"]
        name = (c["name"] or "").strip()
        opted_out = int(c["opted_out"]) == 1
        updated = c["updated_at"]

        pill = "<span class='pill out'>OPTED OUT</span>" if opted_out else "<span class='pill in'>OPTED IN</span>"

        rows_html += f"""
        <tr>
          <td><strong>{phone}</strong><br><span class="muted">{name}</span></td>
          <td>{pill}<br><span class="muted">Updated: {updated}</span></td>
          <td class="rowActions">
            <form method="post" action="/admin/contacts/optin">
              <input type="hidden" name="phone" value="{phone}">
              <button type="submit">Opt In</button>
            </form>
            <form method="post" action="/admin/contacts/optout">
              <input type="hidden" name="phone" value="{phone}">
              <button type="submit" class="btn2">Opt Out</button>
            </form>
            <form method="get" action="/admin/contacts/edit">
              <input type="hidden" name="phone" value="{phone}">
              <button type="submit" class="btn2">Edit</button>
            </form>
            <form method="post" action="/admin/contacts/delete" onsubmit="return confirm('Delete this number?');">
              <input type="hidden" name="phone" value="{phone}">
              <button type="submit" class="btnDanger">Delete</button>
            </form>
          </td>
        </tr>
        """

    body = f"""
    <h2>Admin – Contacts</h2>

    <form method="get" class="searchRow">
      <input name="q" placeholder="Search phone or name" value="{q.replace('"', '&quot;')}" />
      <button type="submit">Search</button>
      <a href="/admin/contacts" class="muted" style="align-self:center">Clear</a>
    </form>

    <div class="card">
      <p class="muted">Showing <strong>{len(contacts)}</strong> contact(s)</p>
      <table>
        <tr>
          <th>Contact</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
        {rows_html if rows_html else "<tr><td colspan='3'>No contacts found.</td></tr>"}
      </table>
    </div>
    """
    return render_admin("Contacts", body)


@app.route("/admin/contacts/optout", methods=["POST"])
def admin_contacts_optout():
    gate = require_admin()
    if gate:
        return gate

    phone = request.form.get("phone") or ""
    phone = normalize_e164(phone, DEFAULT_REGION) or phone
    if phone:
        set_opted_out(phone, 1)
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


@app.route("/admin/contacts/optin", methods=["POST"])
def admin_contacts_optin():
    gate = require_admin()
    if gate:
        return gate

    phone = request.form.get("phone") or ""
    phone = normalize_e164(phone, DEFAULT_REGION) or phone
    if phone:
        set_opted_out(phone, 0)
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


@app.route("/admin/contacts/delete", methods=["POST"])
def admin_contacts_delete():
    gate = require_admin()
    if gate:
        return gate

    phone = request.form.get("phone") or ""
    phone = normalize_e164(phone, DEFAULT_REGION) or phone
    if phone:
        delete_contact(phone)
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


@app.route("/admin/contacts/edit", methods=["GET", "POST"])
def admin_contacts_edit():
    gate = require_admin()
    if gate:
        return gate

    if request.method == "GET":
        phone = request.args.get("phone", "") or ""
        phone_norm = normalize_e164(phone, DEFAULT_REGION) or phone
        c = get_contact_by_phone(phone_norm)
        if not c:
            return redirect(url_for("admin_contacts"))

        body = f"""
        <h2>Edit Contact</h2>
        <div class="card" style="max-width:720px">
          <form method="post">
            <input type="hidden" name="old_phone" value="{c['phone']}">

            <label>Phone number</label>
            <input name="new_phone" value="{c['phone']}" required />

            <label>Name</label>
            <input name="name" value="{(c['name'] or '').replace('"','&quot;')}" />

            <label>Status</label>
            <select name="opted_out">
              <option value="0" {"selected" if int(c["opted_out"])==0 else ""}>OPTED IN</option>
              <option value="1" {"selected" if int(c["opted_out"])==1 else ""}>OPTED OUT</option>
            </select>

            <div style="margin-top:14px">
              <button type="submit">Save</button>
              <a href="/admin/contacts" class="muted" style="margin-left:12px">Cancel</a>
            </div>
          </form>
        </div>
        """
        return render_admin("Edit Contact", body)

    # POST
    old_phone = request.form.get("old_phone") or ""
    new_phone_raw = request.form.get("new_phone") or ""
    new_name_raw = request.form.get("name") or ""
    opted_out_raw = request.form.get("opted_out") or "0"

    old_phone = normalize_e164(old_phone, DEFAULT_REGION) or old_phone
    new_phone = normalize_e164(new_phone_raw, DEFAULT_REGION)
    if not new_phone:
        return render_admin("Edit Contact", "<p class='err'>Invalid phone number.</p><p><a href='/admin/contacts'>Back</a></p>")

    new_name = clean_name(new_name_raw)
    err = update_contact(old_phone, new_phone, new_name)
    if err:
        return render_admin("Edit Contact", f"<p class='err'>{err}</p><p><a href='/admin/contacts'>Back</a></p>")

    try:
        opted_out_val = 1 if str(opted_out_raw).strip() == "1" else 0
        set_opted_out(new_phone, opted_out_val)
    except Exception:
        pass

    export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


# -------------------------
# Twilio Webhook
# -------------------------
@app.route("/sms", methods=["POST"])
def inbound_sms():
    init_db()

    from_number = request.form.get("From", "")
    body = (request.form.get("Body") or "").strip()
    toks = tokens_upper(body)

    phone = normalize_e164(from_number, DEFAULT_REGION)

    resp = MessagingResponse()
    if not phone:
        resp.message("Invalid number. Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    c = get_contact_by_phone(phone)

    if toks & OPTOUT_KEYWORDS:
        if c:
            set_opted_out(phone, 1)
        else:
            # create contact as opted out so it appears in optouts
            add_contact(phone, "")
            set_opted_out(phone, 1)
        export_contacts_csv_and_optouts()
        resp.message("You’re opted out. Reply START to resubscribe.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if toks & HELP_KEYWORDS:
        resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if toks & OPTIN_KEYWORDS:
        if c:
            set_opted_out(phone, 0)
            export_contacts_csv_and_optouts()
            resp.message("You’re subscribed! Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

        # new subscriber
        if ASK_NAME_ON_JOIN:
            # create pending w/ blank name; next message becomes name (simple approach)
            add_contact(phone, "")
            set_opted_out(phone, 0)
            export_contacts_csv_and_optouts()
            resp.message("You’re subscribed! Reply with your first name (example: Joey). Reply STOP to opt out.")
            # We'll interpret the next non-keyword message as name if name is blank.
            return str(resp), 200, {"Content-Type": "application/xml"}
        else:
            add_contact(phone, "")
            set_opted_out(phone, 0)
            export_contacts_csv_and_optouts()
            resp.message("You’re subscribed! Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

    # If opted in and name is empty, treat message as name (only if not a keyword)
    if c and int(c["opted_out"]) == 0 and ASK_NAME_ON_JOIN and not (c["name"] or "").strip():
        name = clean_name(body)
        if name:
            update_contact(phone, phone, name)
            export_contacts_csv_and_optouts()
            resp.message(f"Thanks, {name}! You’re all set. Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

    resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
    return str(resp), 200, {"Content-Type": "application/xml"}
