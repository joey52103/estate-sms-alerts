import csv
import os
import re
import sqlite3
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, request, redirect, url_for, session, render_template_string
import phonenumbers
from twilio.twiml.messaging_response import MessagingResponse

load_dotenv()

app = Flask(__name__)

# IMPORTANT: set this to a long random value in .env for sessions
app.secret_key = os.getenv("SECRET_KEY", "CHANGE_ME_PLEASE")

DEFAULT_REGION = os.getenv("DEFAULT_REGION", "US")

DB_PATH = os.getenv("CONTACTS_DB", "contacts.db")
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


def upsert_contact(
    phone: str,
    *,
    name: Optional[str] = None,
    status: Optional[str] = None,
    pending_name: Optional[int] = None,
) -> None:
    now = datetime.utcnow().isoformat()
    with db() as conn:
        cur = conn.execute(
            "SELECT phone, name, status, pending_name, created_at FROM contacts WHERE phone = ?",
            (phone,),
        )
        row = cur.fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO contacts (phone, name, status, pending_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (phone, name or "", status or "OPTED_IN", int(pending_name or 0), now, now),
            )
        else:
            old_created = row[4]
            new_name = name if name is not None else row[1]
            new_status = status if status is not None else row[2]
            new_pending = int(pending_name) if pending_name is not None else row[3]

            conn.execute(
                """
                UPDATE contacts
                SET name = ?, status = ?, pending_name = ?, created_at = ?, updated_at = ?
                WHERE phone = ?
                """,
                (new_name, new_status, new_pending, old_created, now, phone),
            )


def get_contact(phone: str) -> Optional[dict]:
    with db() as conn:
        cur = conn.execute(
            "SELECT phone, name, status, pending_name FROM contacts WHERE phone = ?",
            (phone,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"phone": row[0], "name": row[1], "status": row[2], "pending_name": row[3]}


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
        for phone, name in opted_in:
            w.writerow([phone, name or ""])

    with open(OPTOUT_FILE, "w", encoding="utf-8") as f:
        for (phone,) in opted_out:
            f.write(phone + "\n")


def tokens_upper(body: str) -> set:
    body = (body or "").strip().upper()
    return set(body.split())


def admin_logged_in() -> bool:
    return session.get("admin_authed") is True


# -------------------------
# Admin UI
# -------------------------
ADMIN_NAV = """
<div class="nav">
  <div class="brand">J Maslanka Estates – Admin</div>
  <div class="links">
    <a href="/admin/add">Add Contact</a>
    <a href="/admin/contacts">Contacts</a>
    <a href="/admin/logout">Logout</a>
  </div>
</div>
"""

BASE_STYLE = """
<style>
body{font-family:system-ui;margin:40px;max-width:1000px}
.card{border:1px solid #ddd;border-radius:14px;padding:18px}
input,select{padding:10px;margin:8px 0;border:1px solid #ccc;border-radius:10px}
button{padding:10px 14px;border:0;border-radius:10px;background:#0b5fff;color:white;cursor:pointer}
small{color:#555}
.ok{color:#0a7a2f;font-weight:600}
.err{color:#b00020;font-weight:600}
a{color:#0b5fff;text-decoration:none}
.nav{display:flex;justify-content:space-between;align-items:center;margin-bottom:22px;padding-bottom:12px;border-bottom:1px solid #eee}
.nav .brand{font-weight:700}
.nav .links a{margin-left:14px}
table{border-collapse:collapse;width:100%;margin-top:12px}
th,td{border:1px solid #ddd;padding:8px;vertical-align:top}
th{background:#f3f3f3;text-align:left}
.badge{padding:2px 10px;border-radius:999px;font-size:12px;font-weight:700;display:inline-block}
.in{background:#e7f7ee;color:#0a7a2f}
.out{background:#fdecec;color:#b00020}
.actions form{display:inline-block;margin-right:6px}
</style>
"""

LOGIN_HTML = f"""
<!doctype html>
<title>Admin Login</title>
{BASE_STYLE}
<h2>Admin Login</h2>
<div class="card">
  <form method="post">
    <label>Username</label><br>
    <input name="user" autocomplete="username" required style="width:100%">
    <label>Password</label><br>
    <input name="password" type="password" autocomplete="current-password" required style="width:100%">
    <button type="submit" style="width:100%">Sign in</button>
    {{% if error %}}<div class="err" style="margin-top:10px">{{{{error}}}}</div>{{% endif %}}
  </form>
</div>
"""

ADD_HTML = f"""
<!doctype html>
<title>Add Contact</title>
{BASE_STYLE}
{ADMIN_NAV}

<div class="card">
  <h2 style="margin-top:0">Add Contact</h2>
  <form method="post">
    <label>Phone number</label><br>
    <input name="phone" placeholder="(412) 555-1234" required style="width:100%">
    <label>Name (optional)</label><br>
    <input name="name" placeholder="Joey" style="width:100%">
    <button type="submit">Add</button>
  </form>

  {{% if ok %}}<p class="ok">{{{{ok}}}}</p>{{% endif %}}
  {{% if error %}}<p class="err">{{{{error}}}}</p>{{% endif %}}

  <p><small>Saved to database and exported to contacts.csv automatically.</small></p>
</div>
"""

CONTACTS_HTML = f"""
<!doctype html>
<title>Contacts</title>
{BASE_STYLE}
{ADMIN_NAV}

<div class="card">
  <h2 style="margin-top:0">Contacts</h2>

  <form method="get">
    <input name="q" placeholder="search phone or name" value="{{{{q}}}}" style="width:320px">
    <select name="status">
      <option value="ALL" {{{{ "selected" if status=="ALL" else "" }}}}>All</option>
      <option value="OPTED_IN" {{{{ "selected" if status=="OPTED_IN" else "" }}}}>Opted In</option>
      <option value="OPTED_OUT" {{{{ "selected" if status=="OPTED_OUT" else "" }}}}>Opted Out</option>
    </select>
    <button type="submit">Search</button>
  </form>

  {{% if ok %}}<p class="ok">{{{{ok}}}}</p>{{% endif %}}
  {{% if error %}}<p class="err">{{{{error}}}}</p>{{% endif %}}

  <table>
    <thead>
      <tr>
        <th>Phone</th>
        <th>Name</th>
        <th>Status</th>
        <th>Actions</th>
        <th>Edit</th>
      </tr>
    </thead>
    <tbody>
      {{% for r in rows %}}
      <tr>
        <td>{{{{r["phone"]}}}}</td>
        <td>{{{{r["name"] or ""}}}}</td>
        <td>
          {{% if r["status"] == "OPTED_IN" %}}
            <span class="badge in">OPTED IN</span>
          {{% else %}}
            <span class="badge out">OPTED OUT</span>
          {{% endif %}}
        </td>

        <td class="actions">
          {{% if r["status"] == "OPTED_IN" %}}
          <form method="post" action="/admin/contacts/optout">
            <input type="hidden" name="phone" value="{{{{r["phone"]}}}}">
            <button type="submit">Opt out</button>
          </form>
          {{% else %}}
          <form method="post" action="/admin/contacts/optin">
            <input type="hidden" name="phone" value="{{{{r["phone"]}}}}">
            <button type="submit">Opt in</button>
          </form>
          {{% endif %}}

          <form method="post" action="/admin/contacts/delete" onsubmit="return confirm('Delete this number?');">
            <input type="hidden" name="phone" value="{{{{r["phone"]}}}}">
            <button type="submit">Delete</button>
          </form>
        </td>

        <td>
          <form method="post" action="/admin/contacts/edit">
            <input type="hidden" name="old_phone" value="{{{{r["phone"]}}}}">
            <div><small>Phone</small></div>
            <input name="new_phone" value="{{{{r["phone"]}}}}" style="width:220px">
            <div><small>Name</small></div>
            <input name="name" value="{{{{r["name"] or ""}}}}" style="width:220px">
            <button type="submit" style="margin-top:6px">Save</button>
          </form>
        </td>
      </tr>
      {{% endfor %}}
    </tbody>
  </table>
</div>
"""


def contact_exists(phone: str) -> bool:
    with db() as conn:
        row = conn.execute("SELECT 1 FROM contacts WHERE phone = ?", (phone,)).fetchone()
        return row is not None


def list_contacts(q: str = "", status: str = "ALL"):
    q = (q or "").strip()
    status = (status or "ALL").upper()

    where = []
    params = []

    if q:
        like = f"%{q}%"
        where.append("(phone LIKE ? OR name LIKE ?)")
        params.extend([like, like])

    if status in ("OPTED_IN", "OPTED_OUT"):
        where.append("status = ?")
        params.append(status)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    sql = f"""
        SELECT phone, name, status
        FROM contacts
        {where_sql}
        ORDER BY updated_at DESC
        LIMIT 500
    """

    with db() as conn:
        return conn.execute(sql, params).fetchall()


def set_status(phone: str, status: str) -> None:
    now = datetime.utcnow().isoformat()
    with db() as conn:
        conn.execute(
            "UPDATE contacts SET status = ?, pending_name = 0, updated_at = ? WHERE phone = ?",
            (status, now, phone),
        )


def delete_contact(phone: str) -> None:
    with db() as conn:
        conn.execute("DELETE FROM contacts WHERE phone = ?", (phone,))


def edit_contact(old_phone: str, new_phone: str, name: str) -> tuple[bool, str]:
    # block changing to a phone that already exists
    if new_phone != old_phone and contact_exists(new_phone):
        return False, "Number already added."

    now = datetime.utcnow().isoformat()
    with db() as conn:
        row = conn.execute("SELECT phone FROM contacts WHERE phone = ?", (old_phone,)).fetchone()
        if not row:
            return False, "Number not found."

        conn.execute(
            """
            UPDATE contacts
            SET phone = ?, name = ?, updated_at = ?
            WHERE phone = ?
            """,
            (new_phone, name, now, old_phone),
        )

    return True, "Updated."


# -------------------------
# Admin routes
# -------------------------
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if admin_logged_in():
        return redirect(url_for("admin_add"))

    error = None
    if request.method == "POST":
        user = (request.form.get("user") or "").strip()
        pw = (request.form.get("password") or "").strip()

        if user == ADMIN_USER and ADMIN_PASSWORD and pw == ADMIN_PASSWORD:
            session["admin_authed"] = True
            return redirect(url_for("admin_add"))
        error = "Invalid login."

    return render_template_string(LOGIN_HTML, error=error)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@app.route("/admin/add", methods=["GET", "POST"])
def admin_add():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))

    ok = None
    error = None

    if request.method == "POST":
        init_db()
        raw_phone = request.form.get("phone") or ""
        raw_name = request.form.get("name") or ""

        phone = normalize_e164(raw_phone, DEFAULT_REGION)
        name = clean_name(raw_name)

        if not phone:
            error = "That phone number looks invalid. Try again (include area code)."
        else:
            if contact_exists(phone):
                error = "Number already added."
            else:
                upsert_contact(phone, name=name, status="OPTED_IN", pending_name=0)
                export_contacts_csv_and_optouts()
                ok = f"Added: {phone}" + (f" ({name})" if name else "")

    return render_template_string(ADD_HTML, ok=ok, error=error)


@app.route("/admin/contacts", methods=["GET"])
def admin_contacts():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))

    init_db()
    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "ALL").strip().upper()

    rows = list_contacts(q=q, status=status)
    return render_template_string(CONTACTS_HTML, rows=rows, q=q, status=status, ok=None, error=None)


@app.route("/admin/contacts/optout", methods=["POST"])
def admin_contacts_optout():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))

    init_db()
    phone = normalize_e164(request.form.get("phone") or "", DEFAULT_REGION)
    if phone:
        set_status(phone, "OPTED_OUT")
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


@app.route("/admin/contacts/optin", methods=["POST"])
def admin_contacts_optin():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))

    init_db()
    phone = normalize_e164(request.form.get("phone") or "", DEFAULT_REGION)
    if phone:
        set_status(phone, "OPTED_IN")
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


@app.route("/admin/contacts/delete", methods=["POST"])
def admin_contacts_delete():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))

    init_db()
    phone = normalize_e164(request.form.get("phone") or "", DEFAULT_REGION)
    if phone:
        delete_contact(phone)
        export_contacts_csv_and_optouts()
    return redirect(url_for("admin_contacts"))


@app.route("/admin/contacts/edit", methods=["POST"])
def admin_contacts_edit():
    if not admin_logged_in():
        return redirect(url_for("admin_login"))

    init_db()
    old_phone = normalize_e164(request.form.get("old_phone") or "", DEFAULT_REGION)
    new_phone = normalize_e164(request.form.get("new_phone") or "", DEFAULT_REGION)
    name = clean_name(request.form.get("name") or "")

    if not old_phone or not new_phone:
        rows = list_contacts()
        return render_template_string(
            CONTACTS_HTML, rows=rows, q="", status="ALL", ok=None, error="Invalid phone number."
        )

    ok, msg = edit_contact(old_phone, new_phone, name)
    export_contacts_csv_and_optouts()

    if ok:
        return redirect(url_for("admin_contacts"))

    rows = list_contacts()
    return render_template_string(CONTACTS_HTML, rows=rows, q="", status="ALL", ok=None, error=msg)


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
