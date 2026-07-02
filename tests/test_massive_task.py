"""utils.massive_task 外壳（parser / 并发 runner / 任务生命周期）的单元测试。"""
from unittest.mock import Mock, patch

import pytest

from utils.massive_task import TaskResult, build_standard_parser, run_concurrently, run_massive_task


class TestBuildStandardParser:
    def test_standard_arguments_present(self):
        parser = build_standard_parser("desc", default_workers=7)
        args = parser.parse_args(["aapl", "msft", "--limit", "5"])
        assert args.symbols == ["aapl", "msft"]
        assert args.market == "US"
        assert args.limit == 5
        assert args.workers == 7
        assert args.all is False

    def test_with_all_false_omits_flag(self):
        parser = build_standard_parser("desc", default_workers=1, with_all=False)
        with pytest.raises(SystemExit):
            parser.parse_args(["--all"])


class TestRunConcurrently:
    def test_collects_outputs(self):
        outputs, counter = run_concurrently(
            [1, 2, 3], lambda x: x * 10, max_workers=2, desc="t",
        )
        assert sorted(outputs) == [10, 20, 30]
        assert counter["FATAL_ERROR"] == 0

    def test_uncaught_exception_counts_fatal_and_continues(self):
        def worker(x):
            if x == 2:
                raise RuntimeError("boom")
            return x

        outputs, counter = run_concurrently([1, 2, 3], worker, max_workers=2, desc="t")
        assert sorted(outputs) == [1, 3]
        assert counter["FATAL_ERROR"] == 1

    def test_batch_item_failure_counts_per_security(self):
        item_a = [Mock(symbol="a"), Mock(symbol="b")]  # 批次：2 支证券
        item_b = [Mock(symbol="c")]

        def worker(batch):
            if len(batch) == 2:
                raise RuntimeError("boom")
            return "ok"

        outputs, counter = run_concurrently([item_a, item_b], worker, max_workers=1, desc="t")
        assert outputs == ["ok"]
        assert counter["FATAL_ERROR"] == 2


class _FakeSource:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _FakeDb:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture()
def patched_runtime():
    source = _FakeSource()
    db = _FakeDb()
    with patch("utils.massive_task.get_massive_api_keys", return_value=["k"]), \
         patch("utils.massive_task.KeyRateLimiter"), \
         patch("utils.massive_task.MassiveSource", return_value=source), \
         patch("utils.massive_task.DatabaseManager", return_value=db), \
         patch("utils.massive_task.setup_logging"):
        yield source, db


class TestRunMassiveTask:
    @staticmethod
    def _parser_factory():
        return build_standard_parser("desc", default_workers=2)

    def test_passes_argv_and_returns_runner_exit_code(self, patched_runtime):
        seen = {}

        def runner(args, source, db):
            seen["symbols"] = args.symbols
            return 0

        code = run_massive_task("t", ["aapl"], self._parser_factory, runner)
        assert code == 0
        assert seen["symbols"] == ["aapl"]

    def test_none_result_normalizes_to_zero(self, patched_runtime):
        assert run_massive_task("t", [], self._parser_factory, lambda a, s, d: None) == 0

    def test_nonzero_exit_code_propagates(self, patched_runtime):
        assert run_massive_task("t", [], self._parser_factory, lambda a, s, d: 1) == 1

    def test_exception_returns_one_and_closes_resources(self, patched_runtime):
        source, db = patched_runtime

        def runner(args, s, d):
            raise RuntimeError("boom")

        assert run_massive_task("t", [], self._parser_factory, runner) == 1
        assert source.closed
        assert db.closed

    def test_resources_closed_on_success(self, patched_runtime):
        source, db = patched_runtime
        run_massive_task("t", [], self._parser_factory, lambda a, s, d: 0)
        assert source.closed
        assert db.closed

    def test_non_us_market_fails_before_runtime_built(self, patched_runtime):
        source, db = patched_runtime
        code = run_massive_task("t", ["--market", "HK"], self._parser_factory, lambda a, s, d: 0)
        assert code == 1
        # enforce_us_market 在构建 source/db 之前抛出
        assert not source.closed and not db.closed

    def test_tuple_result_returns_task_result_with_stats(self, patched_runtime):
        stats = {"processed": 10, "written": 5, "failed": 0}
        result = run_massive_task("t", [], self._parser_factory, lambda a, s, d: (0, stats))
        assert result == 0
        assert isinstance(result, int)
        assert result.stats == stats

    def test_tuple_result_keeps_nonzero_exit_code_and_stats(self, patched_runtime):
        stats = {"processed": 3, "failed": 3}
        result = run_massive_task("t", [], self._parser_factory, lambda a, s, d: (1, stats))
        assert result == 1
        assert result.stats == stats


class TestTaskResult:
    def test_int_semantics_preserved(self):
        # __main__ 的 raise SystemExit(main()) 依赖 int 子类语义
        assert SystemExit(TaskResult(3, {"a": 1})).code == 3
        assert TaskResult(0, {"a": 1}) == 0
        assert not TaskResult(0)
        assert TaskResult(1)

    def test_stats_default_none(self):
        assert TaskResult(0).stats is None
