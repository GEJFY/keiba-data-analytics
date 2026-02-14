"""ワークフローステップバーの単体テスト。"""

import pytest


@pytest.mark.unit
class TestWorkflowBar:
    """workflow_barモジュールのテスト。"""

    def test_workflow_steps_defined(self) -> None:
        """ワークフローステップが定義されていること。"""
        from src.dashboard.components.workflow_bar import WORKFLOW_STEPS

        assert len(WORKFLOW_STEPS) == 5
        keys = [s["key"] for s in WORKFLOW_STEPS]
        assert keys == ["data", "factor", "optimize", "backtest", "betting"]

    def test_step_labels_not_empty(self) -> None:
        """全ステップにラベルがあること。"""
        from src.dashboard.components.workflow_bar import WORKFLOW_STEPS

        for step in WORKFLOW_STEPS:
            assert step["label"], f"Step {step['key']} has empty label"
            assert step["page"], f"Step {step['key']} has empty page"


@pytest.mark.unit
class TestDateDefaults:
    """date_defaultsモジュールのテスト。"""

    def test_factor_analysis_defaults(self) -> None:
        """ファクター分析のデフォルト値が妥当であること。"""
        from src.dashboard.components.date_defaults import factor_analysis_defaults

        date_from, date_to, max_races = factor_analysis_defaults()
        assert len(date_from) == 8  # YYYYMMDD
        assert len(date_to) == 8
        assert max_races == 2000
        assert date_from < date_to

    def test_backtest_defaults(self) -> None:
        """バックテストのデフォルト値が妥当であること。"""
        from src.dashboard.components.date_defaults import backtest_defaults

        date_from, date_to = backtest_defaults()
        assert date_from < date_to

    def test_walk_forward_defaults(self) -> None:
        """Walk-forwardのデフォルト値が妥当であること。"""
        from src.dashboard.components.date_defaults import walk_forward_defaults

        date_from, date_to, n_windows = walk_forward_defaults()
        assert len(date_from) == 8
        assert len(date_to) == 8
        assert n_windows == 5
        assert date_from < date_to
