"""A local failure-handling demo. No real payments, accounts, or network calls."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
from html import escape
import json
from pathlib import Path
import sqlite3
import tempfile


class PayloadConflict(ValueError):
    """An existing identifier was reused for different business data."""


class TemporaryFailure(RuntimeError):
    """An injected, recoverable external-service failure."""


class SimulatedCrash(RuntimeError):
    """An injected crash between an external effect and local acknowledgement."""


def fingerprint(value: dict) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


@contextmanager
def transaction(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def connect(path: Path):
    db = sqlite3.connect(path, isolation_level=None, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


@dataclass(frozen=True)
class OrderRequest:
    event_id: str
    order_id: str
    amount_minor: int
    currency: str = "RUB"

    def validate(self):
        for value in (self.event_id, self.order_id):
            if not isinstance(value, str) or not value.strip() or len(value) > 120:
                raise ValueError("Identifiers must be nonempty strings up to 120 characters")
        if type(self.amount_minor) is not int or not 0 < self.amount_minor <= 2**63 - 1:
            raise ValueError("Amount must be a positive signed 64-bit integer in minor currency units")
        if self.currency not in ("RUB", "USD", "EUR"):
            raise ValueError("Unsupported demo currency")

    def business_data(self):
        return dict(order_id=self.order_id, amount_minor=self.amount_minor, currency=self.currency)


class MockProvider:
    """A separate persistent ledger with an idempotent charge operation."""

    def __init__(self, path: Path, *, lose_first_response=False, always_fail=False):
        self.db = connect(path)
        self.lose_first_response = lose_first_response
        self.always_fail = always_fail
        self.db.execute("""CREATE TABLE IF NOT EXISTS charges (
            idem_key TEXT PRIMARY KEY, payload_hash TEXT NOT NULL,
            reference TEXT NOT NULL UNIQUE)""")

    def charge(self, *, key, payload):
        if self.always_fail:
            raise TemporaryFailure("Injected provider outage")
        digest = fingerprint(payload)
        inserted = False
        with transaction(self.db):
            existing = self.db.execute("SELECT * FROM charges WHERE idem_key=?", (key,)).fetchone()
            if existing:
                if existing["payload_hash"] != digest:
                    raise PayloadConflict("Provider idempotency key has a different payload")
                reference = existing["reference"]
            else:
                reference = "mock-" + hashlib.sha256(key.encode()).hexdigest()[:16]
                self.db.execute("INSERT INTO charges VALUES (?, ?, ?)", (key, digest, reference))
                inserted = True
        # The external effect has committed, but the caller may never receive the response.
        if inserted and self.lose_first_response:
            raise TemporaryFailure("Injected lost response after provider commit")
        return reference

    def count(self):
        return self.db.execute("SELECT count(*) FROM charges").fetchone()[0]

    def close(self):
        self.db.close()


class MockWebhookReceiver:
    """A receiver deduplicating stable notification identifiers."""

    def __init__(self, path: Path, *, failures_remaining=0):
        self.db = connect(path)
        self.failures_remaining = failures_remaining
        self.db.execute("CREATE TABLE IF NOT EXISTS deliveries (event_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL)")

    def receive(self, event_id, payload):
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise TemporaryFailure("Injected webhook receiver outage")
        digest = fingerprint(payload)
        with transaction(self.db):
            row = self.db.execute("SELECT payload_hash FROM deliveries WHERE event_id=?", (event_id,)).fetchone()
            if row and row[0] != digest:
                raise PayloadConflict("Notification identifier has a different payload")
            self.db.execute("INSERT OR IGNORE INTO deliveries VALUES (?, ?)", (event_id, digest))

    def count(self):
        return self.db.execute("SELECT count(*) FROM deliveries").fetchone()[0]

    def close(self):
        self.db.close()


class Workflow:
    MAX_ATTEMPTS = 3

    def __init__(self, path: Path):
        self.path = path
        self.db = connect(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                amount_minor INTEGER NOT NULL CHECK(amount_minor > 0),
                currency TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'paid')),
                provider_reference TEXT);
            CREATE TABLE IF NOT EXISTS input_events (
                event_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL REFERENCES orders(order_id),
                payload_hash TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY,
                kind TEXT NOT NULL CHECK(kind IN ('charge', 'notify')),
                order_id TEXT NOT NULL REFERENCES orders(order_id),
                state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'done', 'dead')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                UNIQUE(kind, order_id));
        """)

    def ingest(self, request: OrderRequest):
        request.validate()
        digest = fingerprint(request.business_data())
        with transaction(self.db):
            event = self.db.execute("SELECT payload_hash FROM input_events WHERE event_id=?", (request.event_id,)).fetchone()
            if event:
                if event[0] != digest:
                    raise PayloadConflict("Input event identifier has a different payload")
                return False
            order = self.db.execute("SELECT * FROM orders WHERE order_id=?", (request.order_id,)).fetchone()
            if order:
                if order["amount_minor"] != request.amount_minor or order["currency"] != request.currency:
                    raise PayloadConflict("Order identifier has different business data")
            else:
                self.db.execute("INSERT INTO orders VALUES (?, ?, ?, 'pending', NULL)",
                                (request.order_id, request.amount_minor, request.currency))
                # The business record and the intent to call the provider commit together.
                self.db.execute("INSERT INTO jobs(kind, order_id) VALUES ('charge', ?)", (request.order_id,))
            self.db.execute("INSERT INTO input_events VALUES (?, ?, ?)", (request.event_id, request.order_id, digest))
        return order is None

    def run_one(self, kind, provider, receiver, *, crash_after_effect=False):
        job = self.db.execute("SELECT * FROM jobs WHERE kind=? AND state='pending' ORDER BY id LIMIT 1", (kind,)).fetchone()
        if job is None:
            return False
        if job["attempts"] >= self.MAX_ATTEMPTS:
            self.db.execute("UPDATE jobs SET state='dead' WHERE id=?", (job["id"],))
            return True
        order = self.db.execute("SELECT * FROM orders WHERE order_id=?", (job["order_id"],)).fetchone()
        self.db.execute("UPDATE jobs SET attempts=attempts+1 WHERE id=?", (job["id"],))
        attempt = job["attempts"] + 1
        payload = dict(order_id=order["order_id"], amount_minor=order["amount_minor"], currency=order["currency"])
        try:
            if kind == "charge":
                reference = provider.charge(key="demo:order:" + order["order_id"], payload=payload)
            else:
                payload.update(status="paid", provider_reference=order["provider_reference"])
                receiver.receive("order.paid:" + order["order_id"], payload)
        except TemporaryFailure as exc:
            self.db.execute("UPDATE jobs SET state=?, last_error=? WHERE id=?",
                            ("dead" if attempt >= self.MAX_ATTEMPTS else "pending", str(exc), job["id"]))
            return True
        if crash_after_effect:
            raise SimulatedCrash("Injected crash after external effect, before local acknowledgement")
        with transaction(self.db):
            if kind == "charge":
                self.db.execute("UPDATE orders SET status='paid', provider_reference=? WHERE order_id=?", (reference, order["order_id"]))
                self.db.execute("INSERT OR IGNORE INTO jobs(kind, order_id) VALUES ('notify', ?)", (order["order_id"],))
            self.db.execute("UPDATE jobs SET state='done', last_error=NULL WHERE id=?", (job["id"],))
        return True

    def drain(self, provider, receiver):
        # No wall-clock sleeps: each step is a deterministic retry opportunity.
        for _ in range(100):
            worked = self.run_one("charge", provider, receiver)
            worked = self.run_one("notify", provider, receiver) or worked
            if not worked:
                return
        raise RuntimeError("Demo did not converge within its bounded run")

    def snapshot(self, provider, receiver):
        orders = [dict(r) for r in self.db.execute("SELECT * FROM orders ORDER BY order_id")]
        jobs = [dict(r) for r in self.db.execute("SELECT kind, state, attempts FROM jobs ORDER BY id")]
        return dict(orders=orders, jobs=jobs, provider_effects=provider.count(),
                    accepted_notifications=receiver.count(),
                    accepted_input_events=self.db.execute("SELECT count(*) FROM input_events").fetchone()[0])

    def close(self):
        self.db.close()


def run_scenario(root: Path, scenario: str):
    root.mkdir(parents=True)
    app = Workflow(root / "service.sqlite")
    provider = MockProvider(root / "provider.sqlite")
    receiver = MockWebhookReceiver(root / "receiver.sqlite", failures_remaining=2 if scenario == "webhook-outage" else 0)
    try:
        request = OrderRequest("event-001", "order-001", 150000)
        app.ingest(request)
        if scenario == "duplicate-request":
            app.ingest(request)
            app.ingest(request)
        elif scenario == "worker-restart":
            try:
                app.run_one("charge", provider, receiver, crash_after_effect=True)
            except SimulatedCrash:
                pass
            app.close()
            provider.close()
            # Re-open both databases, as a new worker process would.
            app = Workflow(root / "service.sqlite")
            provider = MockProvider(root / "provider.sqlite")
        app.drain(provider, receiver)
        result = app.snapshot(provider, receiver)
        result["scenario"] = scenario
        result["passed"] = (len(result["orders"]) == 1
                            and result["orders"][0]["status"] == "paid"
                            and result["provider_effects"] == 1
                            and result["accepted_notifications"] == 1
                            and all(j["state"] == "done" for j in result["jobs"]))
        return result
    finally:
        app.close()
        provider.close()
        receiver.close()


def render_svg(results):
    labels = {
        "duplicate-request": ("Duplicate request", "The same input arrives three times"),
        "worker-restart": ("Worker restart", "Crash after provider success, then restart"),
        "webhook-outage": ("Webhook outage", "Two delivery failures, then recovery"),
    }
    rows = []
    for i, result in enumerate(results):
        title, detail = labels[result["scenario"]]
        y = 140 + i * 132
        attempts = {j["kind"]: j["attempts"] for j in result["jobs"]}
        label = "PASS" if result["passed"] else "FAIL"
        rows.append(f'''<g transform="translate(36 {y})">
<rect width="1128" height="112" rx="12" fill="#101b2c" stroke="#253249"/>
<text x="22" y="34" font-size="23" font-weight="600" fill="#fff">{escape(title)}</text>
<text x="22" y="62" font-size="16" fill="#a3b4c9">{escape(detail)}</text>
<text x="22" y="91" font-size="15" fill="#a3b4c9">Attempts: provider {attempts['charge']} / notification {attempts['notify']}</text>
<text x="555" y="47" font-size="24" fill="#fff">{len(result['orders'])} order</text>
<text x="700" y="47" font-size="24" fill="#fff">{result['provider_effects']} mock charge</text>
<text x="910" y="47" font-size="24" fill="#fff">{result['accepted_notifications']} notice</text>
<text x="1040" y="90" font-size="17" font-weight="700" fill="#01ffc3">{label}</text>
</g>''')
    return '''<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="610" viewBox="0 0 1200 610" role="img" aria-labelledby="title desc">
<title id="title">CKT Labs: integration reliability demo</title>
<desc id="desc">Three executed synthetic scenarios: duplicate input, worker restart, and webhook outage. Each produces one order, one mock charge, and one accepted notification.</desc>
<rect width="1200" height="610" rx="18" fill="#04070e"/>
<g font-family="Segoe UI,Arial,sans-serif">
<text x="36" y="50" font-size="19" font-weight="600" fill="#01ffc3">CKT LABS / ENGINEERING EXAMPLE</text>
<text x="36" y="96" font-size="32" font-weight="700" fill="#fff">Failures happen. The business operation stays consistent.</text>
''' + "\n".join(rows) + '''
<text x="36" y="575" font-size="17" fill="#a3b4c9">Executed locally · Synthetic orders · Mock provider · No real payments or network calls</text>
</g></svg>'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts"))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="cktlabs-demo-") as temp:
        results = [run_scenario(Path(temp) / name, name) for name in
                   ("duplicate-request", "worker-restart", "webhook-outage")]
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "report.json").write_text(json.dumps(dict(simulation=True, results=results), indent=2) + "\n", encoding="utf-8")
    (args.output / "report.svg").write_text(render_svg(results), encoding="utf-8")
    for result in results:
        print(f"{'PASS' if result['passed'] else 'FAIL'}  {result['scenario']}: "
              f"orders={len(result['orders'])}, mock_charges={result['provider_effects']}, "
              f"notifications={result['accepted_notifications']}")
    print(f"Report: {args.output / 'report.json'}")
    if not all(result["passed"] for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
