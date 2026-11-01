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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass


class StepFailed(Exception):
    """A step ran out of attempts or hit a permanent error."""


class BudgetExceeded(Exception):
    """The run went over its cost or step limit."""


class Paused(Exception):
    """The run is waiting on a human or an external event. Resume after signal()."""


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
CREATE TABLE IF NOT EXISTS signals (run TEXT, name TEXT, value TEXT, PRIMARY KEY (run, name));
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

    def map(self, name, fn, items, workers=4, **options):
        """Runs fn over items in parallel. Each item is its own durable step, and order is kept."""
        items = list(items)
        base = self.seq
        self.seq += len(items)
        done, todo = {}, []
        for i, item in enumerate(items):
            step_name = f"{name}[{i}]"
            fingerprint = hashlib.sha256(json.dumps([step_name, [item], {}], sort_keys=True, default=str).encode()).hexdigest()
            row = self.db.execute("SELECT name, args_hash, result FROM steps WHERE run=? AND seq=?", (self.id, base + i + 1)).fetchone()
            if row:
                if (row[0], row[1]) != (step_name, fingerprint):
                    raise NonDeterministic(f"step {base + i + 1}: journal has {row[0]!r}, agent asked for {step_name!r}")
                done[i] = json.loads(row[2])
                self.replayed += 1
            else:
                todo.append((i, item, step_name, fingerprint))
        cost = options.pop("cost", 0.0)
        if self.steps_done + len(todo) > self.budget.max_steps:
            raise BudgetExceeded(f"step limit {self.budget.max_steps} reached")
        if self.cost + cost * len(todo) > self.budget.max_cost:
            raise BudgetExceeded(f"map would pass cost limit {self.budget.max_cost:.4f}")
        retries, backoff = options.pop("retries", 3), options.pop("backoff", 0.5)
        retry_if = options.pop("retry_if", retry_transient)

        def attempt(entry):
            i, item, step_name, _ = entry
            key = f"{self.id}:{base + i + 1}:{step_name}"
            for tries in range(1, retries + 2):
                try:
                    result = fn(item, idempotency_key=key, **options)
                    return entry, json.dumps(result), tries, None
                except Exception as error:
                    if not retry_if(error) or tries > retries:
                        return entry, None, tries, error
                    self.sleep(random.uniform(0, backoff * 2 ** (tries - 1)))

        failure = None
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # SQLite writes stay on this thread, and each result is saved as soon as it lands.
            for future in as_completed([pool.submit(attempt, e) for e in todo]):
                (i, _, step_name, fingerprint), encoded, tries, error = future.result()
                if error is not None:
                    self._log("error", f"{step_name}: {type(error).__name__}: {error}")
                    failure = failure or StepFailed(f"{step_name} failed: {error}")
                    continue
                self.db.execute("INSERT INTO steps VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (self.id, base + i + 1, step_name, fingerprint, encoded, cost, tries, 0.0))
                self.db.commit()
                self.cost += cost
                self.steps_done += 1
                done[i] = json.loads(encoded)
        if failure:
            raise failure
        return [done[i] for i in range(len(items))]

    def wait_for(self, signal_name):
        """Pauses until signal() delivers a value, surviving restarts. Use it for human approval."""
        self.seq += 1
        row = self.db.execute("SELECT result FROM steps WHERE run=? AND seq=?", (self.id, self.seq)).fetchone()
        if row:
            self.replayed += 1
            return json.loads(row[0])
        waiting = self.db.execute("SELECT value FROM signals WHERE run=? AND name=?", (self.id, signal_name)).fetchone()
        if not waiting:
            self.db.execute("UPDATE runs SET status='paused' WHERE id=?", (self.id,))
            self.db.commit()
            raise Paused(signal_name)
        marker = hashlib.sha256(json.dumps([f"signal:{signal_name}", [], {}]).encode()).hexdigest()
        self.db.execute("INSERT INTO steps VALUES (?, ?, ?, ?, ?, 0, 1, 0)", (self.id, self.seq, f"signal:{signal_name}", marker, waiting[0]))
        self.db.execute("UPDATE runs SET status='running' WHERE id=?", (self.id,))
        self.db.commit()
        return json.loads(waiting[0])

    def finish(self, status, result=None):
        self.db.execute("UPDATE runs SET status=?, finished=?, result=? WHERE id=?",
                        (status, time.time(), json.dumps(result), self.id))
        self.db.commit()

    def status(self):
        return self.db.execute("SELECT status FROM runs WHERE id=?", (self.id,)).fetchone()[0]

    def history(self):
        rows = self.db.execute("SELECT seq, name, attempts, cost, seconds FROM steps WHERE run=? ORDER BY seq", (self.id,))
        return [dict(zip(("seq", "name", "attempts", "cost", "seconds"), r)) for r in rows]


def signal(path, run_id, name, value=None):
    """Delivers an approval or external event to a paused run."""
    db = sqlite3.connect(path, timeout=30)
    db.executescript(SCHEMA)
    db.execute("INSERT OR REPLACE INTO signals VALUES (?, ?, ?)", (run_id, name, json.dumps(value)))
    db.commit()
    db.close()


def run_agent(path, run_id, agent, budget=None, **kwargs):
    """Starts or resumes a run. A finished run returns its saved result without executing anything."""
    run = Run(path, run_id, budget, **kwargs)
    row = run.db.execute("SELECT status, result FROM runs WHERE id=?", (run_id,)).fetchone()
    if row[0] == "completed":
        return json.loads(row[1])
    try:
        result = agent(run)
    except Paused:
        raise
    except (StepFailed, BudgetExceeded) as error:
        run.finish("failed", str(error))
        raise
    run.finish("completed", result)
    return result
