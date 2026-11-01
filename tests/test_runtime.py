import pytest

from resumable import Budget, BudgetExceeded, Run, StepFailed, run_agent
from resumable.runtime import NonDeterministic


class Boom(BaseException):
    """Stands in for a crash. It skips except Exception, like a killed process."""


def counter():
    calls = []

    def fn(value, idempotency_key):
        calls.append((value, idempotency_key))
        return value * 2
    return fn, calls


def test_resume_does_not_repeat_completed_steps(tmp_path):
    db = tmp_path / "j.db"
    fn, calls = counter()
    crash = [True]

    def agent(run):
        a = run.step("a", fn, 1)
        if crash[0]:
            raise Boom
        return a + run.step("b", fn, 2)

    with pytest.raises(Boom):
        run_agent(db, "r", agent)
    crash[0] = False
    assert run_agent(db, "r", agent) == 6
    assert [c[0] for c in calls] == [1, 2]


def test_finished_run_returns_saved_result(tmp_path):
    fn, calls = counter()
    agent = lambda run: run.step("a", fn, 1)
    assert run_agent(tmp_path / "j.db", "r", agent) == 2
    assert run_agent(tmp_path / "j.db", "r", agent) == 2
    assert len(calls) == 1


def test_idempotency_key_is_stable_across_retries(tmp_path):
    keys, attempts = [], [0]

    def flaky(idempotency_key):
        keys.append(idempotency_key)
        attempts[0] += 1
        if attempts[0] < 3:
            raise TimeoutError
        return "ok"

    run = Run(tmp_path / "j.db", "r", sleep=lambda s: None)
    assert run.step("x", flaky) == "ok"
    assert len(set(keys)) == 1 and len(keys) == 3
    assert run.history()[0]["attempts"] == 3


def test_permanent_error_fails_fast(tmp_path):
    attempts = [0]

    def bad(idempotency_key):
        attempts[0] += 1
        raise ValueError("bad request")

    with pytest.raises(StepFailed):
        Run(tmp_path / "j.db", "r", sleep=lambda s: None).step("x", bad)
    assert attempts[0] == 1


def test_retries_run_out(tmp_path):
    def down(idempotency_key):
        raise ConnectionError

    with pytest.raises(StepFailed):
        Run(tmp_path / "j.db", "r", sleep=lambda s: None).step("x", down, retries=2)


def test_failed_run_is_recorded(tmp_path):
    def down(idempotency_key):
        raise ValueError

    with pytest.raises(StepFailed):
        run_agent(tmp_path / "j.db", "r", lambda run: run.step("x", down))
    assert Run(tmp_path / "j.db", "r").status() == "failed"


def test_cost_budget_survives_restart(tmp_path):
    db = tmp_path / "j.db"
    fn, _ = counter()
    run = Run(db, "r", Budget(max_cost=1.0))
    run.step("a", fn, 1, cost=0.8)
    resumed = Run(db, "r", Budget(max_cost=1.0))
    assert resumed.cost == pytest.approx(0.8)
    resumed.step("a", fn, 1, cost=0.8)  # replay is free
    with pytest.raises(BudgetExceeded):
        resumed.step("b", fn, 2, cost=0.5)


def test_step_budget(tmp_path):
    fn, _ = counter()
    run = Run(tmp_path / "j.db", "r", Budget(max_steps=1))
    run.step("a", fn, 1)
    with pytest.raises(BudgetExceeded):
        run.step("b", fn, 2)


def test_changed_agent_code_is_detected(tmp_path):
    db = tmp_path / "j.db"
    fn, _ = counter()
    Run(db, "r").step("a", fn, 1)
    with pytest.raises(NonDeterministic):
        Run(db, "r").step("renamed", fn, 1)
    with pytest.raises(NonDeterministic):
        Run(db, "r").step("a", fn, 99)


def test_result_must_be_json(tmp_path):
    def bad(idempotency_key):
        return object()

    with pytest.raises(StepFailed):
        Run(tmp_path / "j.db", "r").step("x", bad)
