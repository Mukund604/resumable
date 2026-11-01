"""Durable agent runs built on a SQLite journal.

The agent is ordinary Python. Each side effect goes through run.step(name, fn, ...), which
stores the result before returning it. After a crash the same function runs again from the
top, and completed steps return their saved results instead of executing again.

The one gap no journal can close is a crash after a side effect happens and before its result
is saved. run.step passes an idempotency key to fn so the external system can dedupe that case.
"""
import hashlib
import json
import random
import sqlite3
import time
from dataclasses import dataclass


class StepFailed(Exception):
    """A step ran out of attempts or hit a permanent error."""


class BudgetExceeded(Exception):
    """The run went over its cost or step limit."""


class NonDeterministic(Exception):
    """A resumed agent asked for different steps than the journal recorded."""


@dataclass
class Budget:
    max_cost: float = float("inf")
    max_steps: int = 10_000


def retry_transient(error):
    """Default policy: retry timeouts and connection errors, fail fast on everything else."""
    return isinstance(error, (TimeoutError, ConnectionError))


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, status TEXT, created REAL, finished REAL, result TEXT);
CREATE TABLE IF NOT EXISTS steps (run TEXT, seq INTEGER, name TEXT, args_hash TEXT, result TEXT, cost REAL,
                                  attempts INTEGER, seconds REAL, PRIMARY KEY (run, seq));
CREATE TABLE IF NOT EXISTS events (run TEXT, at REAL, kind TEXT, detail TEXT);
"""


class Run:
    def __init__(self, path, run_id, budget=None, sleep=time.sleep, seed=None):
        self.db = sqlite3.connect(path, timeout=30)
        self.db.executescript(SCHEMA)
        self.db.execute("PRAGMA journal_mode=WAL")
        # FULL makes each commit reach the disk before the step's result is returned.
        self.db.execute("PRAGMA synchronous=FULL")
        self.id, self.budget, self.sleep = run_id, budget or Budget(), sleep
        self.rng = random.Random(seed)
        self.seq = 0
        self.db.execute("INSERT OR IGNORE INTO runs VALUES (?, 'running', ?, NULL, NULL)", (run_id, time.time()))
        self.db.commit()
        row = self.db.execute("SELECT COALESCE(SUM(cost), 0), COUNT(*) FROM steps WHERE run=?", (run_id,)).fetchone()
        self.cost, self.steps_done = row
        self.replayed = 0

    def _log(self, kind, detail=""):
        self.db.execute("INSERT INTO events VALUES (?, ?, ?, ?)", (self.id, time.time(), kind, detail))
        self.db.commit()

    def step(self, name, fn, *args, retries=3, backoff=0.5, retry_if=retry_transient, cost=0.0, **kwargs):
        """Runs fn(*args, idempotency_key=..., **kwargs) once, durably, and returns its JSON result."""
        self.seq += 1
        seq = self.seq
        fingerprint = hashlib.sha256(json.dumps([name, args, kwargs], sort_keys=True, default=str).encode()).hexdigest()
        row = self.db.execute("SELECT name, args_hash, result FROM steps WHERE run=? AND seq=?", (self.id, seq)).fetchone()
        if row:
            if (row[0], row[1]) != (name, fingerprint):
                # Replaying a stale result into changed code would silently corrupt the run.
                raise NonDeterministic(f"step {seq}: journal has {row[0]!r}, agent asked for {name!r} with these arguments")
            self.replayed += 1
            return json.loads(row[2])
        if self.steps_done >= self.budget.max_steps:
            raise BudgetExceeded(f"step limit {self.budget.max_steps} reached")
        if self.cost + cost > self.budget.max_cost:
            raise BudgetExceeded(f"cost {self.cost + cost:.4f} would pass limit {self.budget.max_cost:.4f}")
        key = f"{self.id}:{seq}:{name}"
        start = time.time()
        for attempt in range(1, retries + 2):
            try:
                result = fn(*args, idempotency_key=key, **kwargs)
                encoded = json.dumps(result)
                break
            except Exception as error:
                self._log("error", f"{name} attempt {attempt}: {type(error).__name__}: {error}")
                if not retry_if(error) or attempt > retries:
                    raise StepFailed(f"{name} failed after {attempt} attempt(s): {error}") from error
                # Full jitter keeps a fleet of agents from retrying in lockstep.
                self.sleep(self.rng.uniform(0, backoff * 2 ** (attempt - 1)))
        self.db.execute("INSERT INTO steps VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (self.id, seq, name, fingerprint, encoded, cost, attempt, time.time() - start))
        self.db.commit()
        self.cost += cost
        self.steps_done += 1
        return result

    def finish(self, status, result=None):
        self.db.execute("UPDATE runs SET status=?, finished=?, result=? WHERE id=?",
                        (status, time.time(), json.dumps(result), self.id))
        self.db.commit()

    def status(self):
        return self.db.execute("SELECT status FROM runs WHERE id=?", (self.id,)).fetchone()[0]

    def history(self):
        rows = self.db.execute("SELECT seq, name, attempts, cost, seconds FROM steps WHERE run=? ORDER BY seq", (self.id,))
        return [dict(zip(("seq", "name", "attempts", "cost", "seconds"), r)) for r in rows]


def run_agent(path, run_id, agent, budget=None, **kwargs):
    """Starts or resumes a run. A finished run returns its saved result without executing anything."""
    run = Run(path, run_id, budget, **kwargs)
    row = run.db.execute("SELECT status, result FROM runs WHERE id=?", (run_id,)).fetchone()
    if row[0] == "completed":
        return json.loads(row[1])
    try:
        result = agent(run)
    except (StepFailed, BudgetExceeded) as error:
        run.finish("failed", str(error))
        raise
    run.finish("completed", result)
    return result
