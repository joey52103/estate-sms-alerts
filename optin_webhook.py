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

# IMPORTANT: set this to a long random value in .env for sessions
# Example: SECRET_KEY=1f5c... (use any long random string)
app.secret_key = os.getenv("SECRET_KEY", "CHANGE_ME_PLEASE")

DEFAULT_REGION = os.getenv("DEFAULT_REGION", "US")

# Support both names so you don't get stuck again:
# Your .env has DB_PATH=contacts.db
DB_PATH = os.getenv("DB_PATH") or os.getenv("CONTACTS_DB", "contacts.db")
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
    return s[:30]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                phone TEXT PRIMARY KEY,
                name TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'OPTED_IN',   -- OPTED_IN or OPTED_OUT
                pending_name INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def get_contact(phone: str) -> Optional[dict]:
    init_db()
    with db() as conn:
        row = conn.execute(
            "SELECT phone, name, status, pending_name, created_at, updated_at FROM contacts WHERE phone = ?",
            (phone,),
        ).fetchone()
        if not row:
            return None
        return dict(row)


def upsert_contact(
    phone: str,
    *,
    name: Optional[str] = None,
    status: Optional[str] = None,
    pending_name: Optional[int] = None,
) -> None:
    init_db()
    now = utc_now()
    with db() as conn:
        row = conn.execute(
            "SELECT phone, name, status, pending_name, created_at FROM contacts WHERE phone = ?",
            (phone,),
        ).fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO contacts (phone, name, status, pending_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (phone, name or "", status or "OPTED_IN", int(pending_name or 0), now, now),
            )
        else:
            old_created = row["created_at"]
            new_name = name if name is not None else row["name"]
            new_status = status if status is not None else row["status"]
            new_pending = int(pending_name) if pending_name is not None else int(row["pending_name"])

            conn.execute(
                """
                UPDATE contacts
                SET name = ?, status = ?, pending_name = ?, created_at = ?, updated_at = ?
                WHERE phone = ?
                """,
                (new_name, new_status, new_pending, old_created, now, phone),
            )


def delete_contact(phone: str) -> None:
    init_db()
    with db() as conn:
        conn.execute("DELETE FROM contacts WHERE phone = ?", (phone,))


def list_contacts(q: str = "") -> List[Dict]:
    init_db()
    q = (q or "").strip()
    with db() as conn:
        if q:
            like = f"%{q}%"
            rows = conn.execute(
                """
                SELECT phone, name, status, pending_name, created_at, updated_at
                FROM contacts
                WHERE phone LIKE ? OR name LIKE ?
                ORDER BY updated_at DESC
                """,
                (like, like),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT phone, name, status, pending_name, created_at, updated_at
                FROM contacts
                ORDER BY updated_at DESC
                """
            ).fetchall()
    return [dict(r) for r in rows]


def export_contacts_csv_and_optouts() -> None:
    init_db()
    with db() as conn:
        opted_in = conn.execute(
            "SELECT phone, name FROM contacts WHERE status = 'OPTED_IN' ORDER BY updated_at DESC"
        ).fetchall()
        opted_out = conn.execute(
            "SELECT phone FROM contacts WHERE status = 'OPTED_OUT' ORDER BY updated_at DESC"
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
# Admin UI (with top nav)
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
    input{padding:10px;margin:6px 0;border:1px solid #ccc;border-radius:10px;width:100%}
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


@app.route("/_routes")
def show_routes():
    # handy for debugging (you already used this)
    routes = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint != "static":
            routes.append(str(rule))
    routes.sort()
    return "\n".join(routes), 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if admin_logged_in():
        return redirect(url_for("admin_add"))

    error = ""
    if request.method == "POST":
        user = (request.form.get("user") or "").strip()
        pw = (request.form.get("password") or "").strip()

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
            existing = get_contact(phone)
            if existing:
                error = "Number already added."
            else:
                upsert_contact(phone, name=name, status="OPTED_IN", pending_name=0)
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
        status = c["status"]
        pill = "<span class='pill in'>OPTED IN</span>" if status == "OPTED_IN" else "<span class='pill out'>OPTED OUT</span>"
        phone = c["phone"]
        name = (c["name"] or "").strip()
        created = c["created_at"]
        updated = c["updated_at"]

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
        upsert_contact(phone, status="OPTED_OUT", pending_name=0)
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
        upsert_contact(phone, status="OPTED_IN", pending_name=0)
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
        c = get_contact(phone_norm)
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
            <select name="status" style="padding:10px;border-radius:10px;border:1px solid #ccc;width:100%">
              <option value="OPTED_IN" {"selected" if c["status"]=="OPTED_IN" else ""}>OPTED_IN</option>
              <option value="OPTED_OUT" {"selected" if c["status"]=="OPTED_OUT" else ""}>OPTED_OUT</option>
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
    new_status = (request.form.get("status") or "OPTED_IN").strip().upper()

    old_phone = normalize_e164(old_phone, DEFAULT_REGION) or old_phone
    new_phone = normalize_e164(new_phone_raw, DEFAULT_REGION)

    if not new_phone:
        # re-render quick error page
        body = """
        <h2>Edit Contact</h2>
        <p class="err">Invalid phone number.</p>
        <p><a href="/admin/contacts">Back</a></p>
        """
        return render_admin("Edit Contact", body)

    if new_status not in {"OPTED_IN", "OPTED_OUT"}:
        new_status = "OPTED_IN"

    new_name = clean_name(new_name_raw)

    # If changing phone to one that already exists (and it's not the same record)
    if new_phone != old_phone and get_contact(new_phone):
        body = f"""
        <h2>Edit Contact</h2>
        <p class="err">That phone number already exists.</p>
        <p><a href="/admin/contacts/edit?phone={old_phone}">Back to edit</a></p>
        """
        return render_admin("Edit Contact", body)

    # update record
    existing = get_contact(old_phone)
    if not existing:
        return redirect(url_for("admin_contacts"))

    created_at = existing["created_at"]

    with db() as conn:
        # delete old if phone changed
        if new_phone != old_phone:
            conn.execute("DELETE FROM contacts WHERE phone = ?", (old_phone,))
            conn.execute(
                """
                INSERT INTO contacts (phone, name, status, pending_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (new_phone, new_name, new_status, 0, created_at, utc_now()),
            )
        else:
            conn.execute(
                """
                UPDATE contacts
                SET name = ?, status = ?, pending_name = 0, updated_at = ?
                WHERE phone = ?
                """,
                (new_name, new_status, utc_now(), old_phone),
            )

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
    body_upper_tokens = tokens_upper(body)

    phone = normalize_e164(from_number, DEFAULT_REGION)

    resp = MessagingResponse()
    if not phone:
        resp.message("Invalid number. Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    contact = get_contact(phone)

    if body_upper_tokens & OPTOUT_KEYWORDS:
        upsert_contact(phone, status="OPTED_OUT", pending_name=0)
        export_contacts_csv_and_optouts()
        resp.message("You’re opted out. Reply START to resubscribe.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if body_upper_tokens & HELP_KEYWORDS:
        resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if body_upper_tokens & OPTIN_KEYWORDS:
        need_name = 1 if ASK_NAME_ON_JOIN else 0
        existing_name = (contact["name"] if contact else "").strip() if contact else ""
        if existing_name:
            need_name = 0

        upsert_contact(phone, status="OPTED_IN", pending_name=need_name)
        export_contacts_csv_and_optouts()

        if need_name:
            resp.message("You’re subscribed! Reply with your first name (example: Joey). Reply STOP to opt out.")
        else:
            resp.message("You’re subscribed! Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    if contact and contact["status"] == "OPTED_IN" and int(contact["pending_name"]) == 1:
        name = clean_name(body)
        if not name:
            resp.message("Please reply with just your first name (example: Joey). Reply STOP to opt out.")
            return str(resp), 200, {"Content-Type": "application/xml"}

        upsert_contact(phone, name=name, pending_name=0)
        export_contacts_csv_and_optouts()
        resp.message(f"Thanks, {name}! You’re all set. Reply STOP to opt out.")
        return str(resp), 200, {"Content-Type": "application/xml"}

    resp.message("Reply JOIN to subscribe. Reply STOP to opt out.")
    return str(resp), 200, {"Content-Type": "application/xml"}
