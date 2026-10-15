"""Crash only a dedicated publisher in the local LedgerFlow Compose demonstration."""
import json
import subprocess
import time
import urllib.request

from demo import accept, get


def compose(*args, check=True):
    return subprocess.run(["docker", "compose", *args], check=check, capture_output=True, text=True, timeout=90)


def sql(query):
    return compose("exec", "-T", "database", "psql", "-U", "ledgerflow", "-d", "ledgerflow", "-Atc", query).stdout.strip()


def until(predicate, reason):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(reason)


def main():
    current = subprocess.run(["docker", "context", "inspect"], check=True, capture_output=True, text=True)
    assert json.loads(current.stdout)[0]["Endpoints"]["docker"]["Host"].startswith("unix://"), "Local Unix-socket Docker context required"
    until(lambda: sql("SELECT count(*) FROM outbox WHERE published_at IS NULL") == "0" and sql("SELECT count(*) FROM deliveries WHERE state='pending'") == "0", "Existing demo work did not drain")
    try:
        compose("stop", "publisher", "consumer")
        transfer = accept("alice", "bob", 25)
        event = transfer["id"]
        assert sql(f"SELECT count(*) FROM outbox WHERE event_id='{event}' AND published_at IS NULL") == "1"
        crashed = compose("run", "--rm", "--no-deps", "-e", "ENABLE_LOCAL_FAULTS=1", "-e", "LOCAL_CRASH_AFTER_PUBLISH=1", "publisher", check=False)
        assert crashed.returncode == 77, (crashed.returncode, crashed.stdout, crashed.stderr)
        assert sql(f"SELECT count(*) FROM deliveries WHERE event_id='{event}'") == "1"
        assert sql(f"SELECT count(*) FROM outbox WHERE event_id='{event}' AND published_at IS NULL") == "1"
        print(json.dumps({"phase": "crashed", "publisher_exit": 77, "event_id": event, "relay_deliveries": 1, "committed_outbox_still_pending": True}))
        compose("start", "publisher", "consumer")
        until(lambda: get("/transfers/" + event)["state"] == "posted" and sql(f"SELECT count(*) FROM outbox WHERE event_id='{event}' AND published_at IS NULL") == "0", "Publisher recovery failed")
        until(lambda: sql(f"SELECT count(*) FROM deliveries WHERE event_id='{event}' AND state='pending'") == "0", "Duplicate deliveries did not drain")
        deliveries = int(sql(f"SELECT count(*) FROM deliveries WHERE event_id='{event}'"))
        journals = int(sql(f"SELECT count(*) FROM journals WHERE transfer_id='{event}'"))
        inbox = int(sql(f"SELECT count(*) FROM inbox WHERE event_id='{event}'"))
        assert deliveries >= 2 and journals == 1 and inbox == 1, (deliveries, journals, inbox)
        # Check this transfer's immutable accounting effect; unrelated valid transfers
        # may also change whole-account snapshots while recovery runs.
        posting = json.loads(sql(f"SELECT jsonb_object_agg(account_id,amount) FROM entries WHERE journal_id IN (SELECT id FROM journals WHERE transfer_id='{event}')"))
        assert posting == {"alice": -25, "bob": 25}, posting
        report = get("/reconcile")
        assert report["balanced"], report
        print(json.dumps({"phase": "recovered", "event_id": event, "relay_deliveries": deliveries, "journals": journals, "inbox": inbox, "reconciliation": report}))
    finally:
        compose("start", "publisher", "consumer")


if __name__ == "__main__":
    main()
