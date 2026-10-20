import sqlite3
from pathlib import Path
import tempfile
import unittest

from demo import (MockProvider, MockWebhookReceiver, OrderRequest, PayloadConflict,
                  SimulatedCrash, TemporaryFailure, Workflow)


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.app = Workflow(self.root / "service.sqlite")
        self.provider = MockProvider(self.root / "provider.sqlite")
        self.receiver = MockWebhookReceiver(self.root / "receiver.sqlite")
        self.request = OrderRequest("event-1", "order-1", 150000)

    def tearDown(self):
        self.app.close()
        self.provider.close()
        self.receiver.close()
        self.temp.cleanup()

    def assert_complete(self):
        self.app.drain(self.provider, self.receiver)
        snapshot = self.app.snapshot(self.provider, self.receiver)
        self.assertEqual(snapshot["provider_effects"], 1)
        self.assertEqual(snapshot["accepted_notifications"], 1)
        self.assertEqual(snapshot["orders"][0]["status"], "paid")
        self.assertTrue(all(j["state"] == "done" for j in snapshot["jobs"]))

    def test_duplicate_input_has_one_business_effect(self):
        self.assertTrue(self.app.ingest(self.request))
        self.assertFalse(self.app.ingest(self.request))
        self.assertFalse(self.app.ingest(OrderRequest("event-2", "order-1", 150000)))
        self.assert_complete()

    def test_event_identifier_cannot_change_amount(self):
        self.app.ingest(self.request)
        with self.assertRaises(PayloadConflict):
            self.app.ingest(OrderRequest("event-1", "order-1", 150001))
        self.assert_complete()

    def test_order_identifier_cannot_change_currency(self):
        self.app.ingest(self.request)
        with self.assertRaises(PayloadConflict):
            self.app.ingest(OrderRequest("event-2", "order-1", 150000, "USD"))
        self.assertEqual(self.app.snapshot(self.provider, self.receiver)["accepted_input_events"], 1)

    def test_order_and_job_roll_back_together(self):
        self.app.db.execute("""CREATE TRIGGER reject_job BEFORE INSERT ON jobs
                            BEGIN SELECT RAISE(ABORT, 'injected transaction failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.app.ingest(self.request)
        snapshot = self.app.snapshot(self.provider, self.receiver)
        self.assertEqual(snapshot["orders"], [])
        self.assertEqual(snapshot["accepted_input_events"], 0)

    def test_restart_after_external_effect_does_not_duplicate_charge(self):
        self.app.ingest(self.request)
        with self.assertRaises(SimulatedCrash):
            self.app.run_one("charge", self.provider, self.receiver, crash_after_effect=True)
        self.app.close()
        self.provider.close()
        self.app = Workflow(self.root / "service.sqlite")
        self.provider = MockProvider(self.root / "provider.sqlite")
        self.assert_complete()

    def test_lost_provider_response_retries_same_operation(self):
        self.provider.lose_first_response = True
        self.app.ingest(self.request)
        self.assert_complete()
        self.assertEqual(self.app.snapshot(self.provider, self.receiver)["jobs"][0]["attempts"], 2)

    def test_receiver_recovery_keeps_charge_count_constant(self):
        self.receiver.failures_remaining = 2
        self.app.ingest(self.request)
        self.assert_complete()
        self.assertEqual(self.app.snapshot(self.provider, self.receiver)["jobs"][1]["attempts"], 3)

    def test_crash_after_notification_is_deduplicated_by_receiver(self):
        self.app.ingest(self.request)
        self.app.run_one("charge", self.provider, self.receiver)
        with self.assertRaises(SimulatedCrash):
            self.app.run_one("notify", self.provider, self.receiver, crash_after_effect=True)
        self.assert_complete()

    def test_exhausted_provider_attempts_stop_without_false_success(self):
        self.provider.always_fail = True
        self.app.ingest(self.request)
        self.app.drain(self.provider, self.receiver)
        snapshot = self.app.snapshot(self.provider, self.receiver)
        self.assertEqual(snapshot["orders"][0]["status"], "pending")
        self.assertEqual(snapshot["jobs"][0]["state"], "dead")
        self.assertEqual(snapshot["jobs"][0]["attempts"], 3)
        self.assertEqual(snapshot["provider_effects"], 0)
        self.assertEqual(snapshot["accepted_notifications"], 0)

    def test_provider_rejects_same_key_with_different_payload(self):
        self.provider.charge(key="key", payload={"amount_minor": 100})
        with self.assertRaises(PayloadConflict):
            self.provider.charge(key="key", payload={"amount_minor": 101})
        self.assertEqual(self.provider.count(), 1)

    def test_invalid_amounts_create_no_work(self):
        for amount in (0, -1, True, 1.5, "100", 2**63):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                self.app.ingest(OrderRequest("event-1", "order-1", amount))
        self.assertEqual(self.app.snapshot(self.provider, self.receiver)["orders"], [])


if __name__ == "__main__":
    unittest.main()
