import os
import pathlib
import subprocess
import sys

SCENARIOS = {
    "001": ("LF_UNSAFE_DUPLICATE", "TestIncident001DuplicatePosting", "duplicate delivery created duplicate business posting"),
    "002": ("LF_UNSAFE_PUBLISH", "TestIncident002UncertainRelayAcknowledgement", "uncertain publication silently discarded committed pending work"),
    "003": ("LF_UNSAFE_CONCURRENCY", "TestIncident003ConcurrentSpend", "concurrent spend violated accounting invariant"),
}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in SCENARIOS:
        raise SystemExit("Usage: python3 scripts/incident.py 001|002|003")
    incident = sys.argv[1]
    key, test, reason = SCENARIOS[incident]
    env = os.environ.copy()
    for variable, *_ in SCENARIOS.values():
        env.pop(variable, None)
    command = ["go", "test", "-race", "-tags=integration", "-count=1", "-run", "^" + test + "$", "-v", "./internal/ledger"]
    output = pathlib.Path("evidence-local")
    output.mkdir(exist_ok=True)
    env[key] = "1"
    broken = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
    text = broken.stdout + broken.stderr
    (output / (incident + "-broken.txt")).write_text(text)
    assert broken.returncode != 0 and reason in text, "Unexpected failure: " + text
    env.pop(key)
    fixed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
    passed = fixed.stdout + fixed.stderr
    (output / (incident + "-fixed.txt")).write_text(passed)
    assert fixed.returncode == 0, passed
    print("INCIDENT-" + incident + ": unsafe policy failed for intended reason; corrected policy passed.")
    print(text.strip())
    print(passed.strip())


if __name__ == "__main__":
    main()
