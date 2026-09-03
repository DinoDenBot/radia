import threading
import time
import unittest

from radia import agent_loop


class _BoundClient:
    def __init__(self, token, calls):
        self.token = token
        self.run = None
        self.calls = calls

    def renew(self, lease, lease_seconds=None):
        self.calls.append(("renew", self.token, lease["leaseId"]))
        return {"status": "renewed"}

    def ack(self, lease, result=None, idempotency_key=None):
        self.calls.append(("ack", self.token, lease["leaseId"]))
        return {"status": "consumed"}

    def nack(self, lease, backoff_seconds=None, idempotency_key=None):
        self.calls.append(("nack", self.token, lease["leaseId"]))
        return {"status": "available"}


class _RotatingClient:
    def __init__(self):
        self.token = "run-token-that-claimed"
        self.run = "run:claiming"
        self._exchange_lock = threading.RLock()
        self.calls = []
        self.claimed = False

    def keep_alive(self, stop, on_lost=None):
        return threading.Thread()

    def watch(self, pattern, stop):
        if False:
            yield None

    def take(self, selector, lease_seconds=None):
        if self.claimed:
            return None
        self.claimed = True
        threading.Timer(0.1, self._rotate).start()
        return {"record": {"id": "record-1", "kind": "task", "body": {}},
                "lease": {"leaseId": "lease-1", "epoch": 1, "ownerRun": "run:claiming"}}

    def _rotate(self):
        self.token = "replacement-run-token"
        self.run = "run:replacement"

    def with_token(self, token):
        return _BoundClient(token, self.calls)


class ClaimBoundAgentLoopTest(unittest.TestCase):
    def test_rotation_during_active_lease_does_not_move_lease_operations(self):
        client = _RotatingClient()
        stop = threading.Event()

        def handle(_record, claim_client, _fenced):
            time.sleep(1.2)  # one lease heartbeat, after the parent rotates
            self.assertEqual("run-token-that-claimed", claim_client.token)
            stop.set()
            return {"kind": "result", "body": {"ok": True}}

        agent_loop(client, "rotation-regression", [{"kind": "task"}], handle,
                   lease_seconds=3, poll_seconds=0.01, stop=stop)
        self.assertEqual("replacement-run-token", client.token)
        self.assertIn(("renew", "run-token-that-claimed", "lease-1"), client.calls)
        self.assertIn(("ack", "run-token-that-claimed", "lease-1"), client.calls)
        self.assertFalse(any(token == "replacement-run-token" for _, token, _ in client.calls))


if __name__ == "__main__":
    unittest.main()
