import json
import time
import urllib.request
import uuid

BASE = "http://127.0.0.1:8090"


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=5) as response:
        return json.load(response)


def accept(source, destination, amount):
    body = json.dumps({"from": source, "to": destination, "amount_minor": amount, "currency": "USD"}).encode()
    request = urllib.request.Request(BASE + "/transfers", data=body, headers={"Content-Type": "application/json", "Idempotency-Key": "demo-" + uuid.uuid4().hex})
    with urllib.request.urlopen(request, timeout=5) as response:
        assert response.status == 202
        return json.load(response)


def main():
    transfers = [accept("alice", "bob", 100), accept("bob", "carol", 75), accept("carol", "alice", 25)]
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        outcomes = [get("/transfers/" + transfer["id"]) for transfer in transfers]
        if all(outcome["state"] != "accepted" for outcome in outcomes):
            break
        time.sleep(0.1)
    assert all(outcome["state"] == "posted" for outcome in outcomes), outcomes
    report = get("/reconcile")
    assert report["balanced"], report
    print(json.dumps({"transfers": outcomes, "reconciliation": report, "accounts": get("/accounts")}, indent=2))


if __name__ == "__main__":
    main()
