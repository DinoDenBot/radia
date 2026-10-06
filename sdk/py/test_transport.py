import http.client
import io
import json
import threading
import unittest
import urllib.error
from unittest.mock import Mock, patch

from radia import RadiaClient, RadiaError, agent_loop


def response(body):
    return io.BytesIO(json.dumps(body).encode())


class TransportRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.client = RadiaClient("http://localhost:7788", token="test-run")

    def test_connect_and_body_failures_are_retryable(self):
        failures = [TimeoutError("timed out"), ConnectionResetError("reset"),
                    http.client.RemoteDisconnected("closed"), http.client.IncompleteRead(b"partial"),
                    urllib.error.URLError("offline")]
        for failure in failures:
            for phase in ("connect", "body"):
                with self.subTest(failure=type(failure).__name__, phase=phase):
                    res = Mock()
                    res.__enter__ = Mock(return_value=res)
                    res.__exit__ = Mock(return_value=False)
                    res.read.side_effect = failure
                    with patch("radia.urllib.request.urlopen", side_effect=failure if phase == "connect" else None,
                               return_value=res):
                        with self.assertRaises(RadiaError) as caught:
                            self.client.health()
                    self.assertEqual((0, "unreachable"), (caught.exception.status, caught.exception.code))

    def test_http_status_and_invalid_json_are_preserved(self):
        failure = urllib.error.HTTPError(self.client.base, 403, "Forbidden", {},
                                        response({"title": "forbidden", "detail": "revoked"}))
        with patch("radia.urllib.request.urlopen", side_effect=failure):
            with self.assertRaises(RadiaError) as caught:
                self.client.health()
        self.assertEqual((403, "forbidden"), (caught.exception.status, caught.exception.code))
        with patch("radia.urllib.request.urlopen", return_value=io.BytesIO(b"invalid json")):
            with self.assertRaises(json.JSONDecodeError):
                self.client.health()

    def test_token_renewal_recovers_after_socket_timeout(self):
        stop = threading.Event()
        waits = []

        def wait(delay):
            waits.append(delay)
            if len(waits) == 2:
                stop.set()
            return stop.is_set()

        lost = Mock()
        with patch.object(stop, "wait", side_effect=wait), patch("radia.urllib.request.urlopen", side_effect=[
            TimeoutError("timed out"), response({"principal": "run:test"}), response({})
        ]) as transport:
            thread = self.client.keep_alive(stop, lost)
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(3, transport.call_count)
        self.assertEqual([30.0, 60.0], waits)
        lost.assert_not_called()

    def test_dropped_error_body_preserves_authorization_status(self):
        body = Mock()
        body.read.side_effect = TimeoutError("timed out")
        failure = urllib.error.HTTPError(self.client.base, 403, "Forbidden", {}, body)
        with patch("radia.urllib.request.urlopen", side_effect=failure):
            with self.assertRaises(RadiaError) as caught:
                self.client.health()
        self.assertEqual(403, caught.exception.status)

    def test_stopped_run_still_ends_renewal(self):
        stop = threading.Event()
        lost = Mock()
        failure = urllib.error.HTTPError(self.client.base, 409, "Conflict", {},
                                        response({"title": "run_stopped", "detail": "stopped"}))
        with patch("radia.urllib.request.urlopen", side_effect=[response({"principal": "run:test"}), failure]):
            thread = self.client.keep_alive(stop, lost)
            thread.join(2)
        self.assertFalse(thread.is_alive())
        lost.assert_called_once_with("stopped")

    def test_poll_loop_recovers_and_settles_after_timeout(self):
        stop = threading.Event()
        claim = {"record": {"id": "record-1", "kind": "task", "body": {}},
                 "lease": {"leaseId": "lease-1", "epoch": 1, "ownerRun": "run:test"}}

        def handle(record, client):
            self.assertEqual("record-1", record["id"])
            stop.set()

        with patch.object(self.client, "keep_alive"), patch.object(self.client, "watch", return_value=iter(())), \
                patch("radia.urllib.request.urlopen", side_effect=[TimeoutError("timed out"), response(claim),
                                                                   response({"status": "consumed"})]) as transport:
            agent_loop(self.client, "transport-regression", [{"kind": "task"}], handle,
                       poll_seconds=0.001, stop=stop, log=lambda _: None)
        self.assertEqual(3, transport.call_count)
        self.assertTrue(transport.call_args.args[0].full_url.endswith("/ack"))

    def test_watch_reconnects_after_body_drop_with_cursor(self):
        class DroppedStream(io.BytesIO):
            def __next__(self):
                line = self.readline()
                if line:
                    return line
                raise http.client.RemoteDisconnected("closed midstream")

        first = DroppedStream(b'id: cursor-1\ndata: {"seq": 1}\n\n')
        second = io.BytesIO(b'id: cursor-2\ndata: {"seq": 2}\n\n')
        stop = threading.Event()
        with patch("radia.time.sleep"), patch.object(stop, "wait", return_value=False), \
                patch("radia.urllib.request.urlopen", side_effect=[response({"watchId": "watch-1"}), first, second]) as transport:
            events = self.client.watch({"kind": "task"}, stop)
            self.assertEqual({"seq": 1}, next(events))
            self.assertEqual({"seq": 2}, next(events))
            self.assertEqual("cursor-1", transport.call_args.args[0].get_header("Last-event-id"))
            stop.set()
            events.close()

    def test_watch_revocation_remains_terminal(self):
        revoked = io.BytesIO(b'event: revoked\ndata: stopped\n\n')
        with patch("radia.urllib.request.urlopen", side_effect=[response({"watchId": "watch-1"}), revoked]) as transport:
            with self.assertRaises(RadiaError) as caught:
                next(self.client.watch({"kind": "task"}))
        self.assertEqual(403, caught.exception.status)
        self.assertEqual(2, transport.call_count)


if __name__ == "__main__":
    unittest.main()
