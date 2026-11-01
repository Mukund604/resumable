"""Kills a real agent process with SIGKILL at random moments and audits the outcome.

Each trial runs a 12 step agent in a subprocess. Steps call a fake payment ledger, a separate
SQLite file standing in for an external API, that charges an account. The parent kills the agent
at a random time, restarts it, and repeats until it finishes. Ground truth is one charge per step.

Three designs are compared:
  restart     no journal, the agent reruns from the top after every crash
  journal     durable steps, but the external API ignores idempotency keys
  journal+key durable steps, and the external API dedupes on the idempotency key
"""
import json
import pathlib
import random
import signal
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

from resumable import Run, run_agent

STEPS = 12


def charge(ledger, step, idempotency_key=None, dedupe=True):
    db = sqlite3.connect(ledger, timeout=30)
    time.sleep(0.01)  # the network round trip, where a crash leaves the outcome unknown
    key = idempotency_key if dedupe and idempotency_key else None
    if key:
        db.execute("INSERT OR IGNORE INTO charges (key, step) VALUES (?, ?)", (key, step))
    else:
        db.execute("INSERT INTO charges (key, step) VALUES (NULL, ?)", (step,))
    db.commit()
    db.close()
    time.sleep(0.01)  # the response on its way back
    return step


def child(mode, journal, ledger):
    if mode == "restart":
        for step in range(STEPS):
            charge(ledger, step, dedupe=False)
        return

    def agent(run):
        for step in range(STEPS):
            run.step(f"charge{step}", charge, ledger, step, dedupe=mode == "journal+key", retries=0)
        return "done"

    run_agent(journal, "trial", agent)


def trial(mode, rng):
    with tempfile.TemporaryDirectory() as folder:
        journal, ledger = f"{folder}/journal.db", f"{folder}/ledger.db"
        db = sqlite3.connect(ledger)
        db.execute("CREATE TABLE charges (id INTEGER PRIMARY KEY, key TEXT UNIQUE, step INTEGER)")
        db.commit()
        db.close()
        crashes = 0
        while True:
            process = subprocess.Popen([sys.executable, "-m", "benchmarks.chaos", "--child", mode, journal, ledger])
            # Kill at a random point in most attempts, and let the later ones finish.
            if crashes < 4 and rng.random() < 0.85:
                time.sleep(rng.uniform(0.0, 0.35))
                if process.poll() is None:
                    process.send_signal(signal.SIGKILL)
                    crashes += 1
            if process.wait() == 0:
                break
        rows = sqlite3.connect(ledger).execute("SELECT step, COUNT(*) FROM charges GROUP BY step").fetchall()
        counts = dict(rows)
        return dict(crashes=crashes, extra=sum(max(0, counts.get(s, 0) - 1) for s in range(STEPS)),
                    missing=sum(s not in counts for s in range(STEPS)))


def overhead(steps=500):
    """Time added per durable step on this machine, which depends on the disk's sync speed."""
    with tempfile.TemporaryDirectory() as folder:
        run = Run(f"{folder}/j.db", "o")
        noop = lambda idempotency_key: 1
        start = time.perf_counter()
        for i in range(steps):
            run.step("n", noop)
        return (time.perf_counter() - start) / steps * 1000


def main(trials=100):
    rng = random.Random(0)
    out = {}
    for mode in ("restart", "journal", "journal+key"):
        rows = [trial(mode, rng) for _ in range(trials)]
        out[mode] = dict(trials=trials, kills=sum(r["crashes"] for r in rows),
                         mean_duplicate_charges=statistics.mean(r["extra"] for r in rows),
                         trials_with_duplicates=sum(r["extra"] > 0 for r in rows),
                         missing_charges=sum(r["missing"] for r in rows),
                         duplicates_by_kills={str(k): statistics.mean(r["extra"] for r in rows if r["crashes"] == k)
                                              for k in sorted({r["crashes"] for r in rows})})
        print(mode, out[mode], flush=True)
    out["ms_per_step"] = overhead()
    # Imported here on purpose: a slow import in the child would push kills before any step runs.
    from benchmarks.charts import save
    pathlib.Path("benchmarks/results.json").write_text(json.dumps(out, indent=2))

    def draw(fig, theme):
        ax = fig.add_subplot()
        names = [("restart", "Restart from the top"), ("journal", "Journal only"), ("journal+key", "Journal + idempotency key")]
        for i, (mode, label) in enumerate(names):
            row = out[mode]["duplicates_by_kills"]
            keys = sorted(row, key=int)
            ax.plot([int(k) for k in keys], [row[k] for k in keys], color=theme["series"][i], lw=2, marker="o", ms=7, label=label)
        ax.set_xlabel("Times the process was killed during the run (SIGKILL)")
        ax.set_ylabel("Duplicate charges per run")
        ax.set_xticks(range(0, 5))
        ax.set_ylim(bottom=-0.5)
        ax.set_title(f"Crash test: 12 step agent, {trials} runs per design")
        ax.legend(loc="upper left")
    save(draw, "crash-test")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        child(*sys.argv[2:5])
    else:
        main()
