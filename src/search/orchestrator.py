"""自律モデル探索の全体制御。"""

import time

import numpy as np
from loguru import logger

from src.data.db import DatabaseManager
from src.search.config import SearchConfig, SearchSpace
from src.search.reporter import SearchReporter, SearchSummary
from src.search.result_store import ResultStore
from src.search.trial_runner import TrialRunner


class ModelSearchOrchestrator:
    """自律モデル探索オーケストレーター。"""

    def __init__(
        self,
        jvlink_db: DatabaseManager,
        ext_db: DatabaseManager,
        search_config: SearchConfig,
    ) -> None:
        self._jvlink_db = jvlink_db
        self._ext_db = ext_db
        self._config = search_config
        self._space = SearchSpace()
        self._store = ResultStore(ext_db)
        self._runner = TrialRunner(jvlink_db, ext_db)
        self._reporter = SearchReporter(self._store)
        self._rng = np.random.default_rng(search_config.random_seed)

    def run(self) -> SearchSummary:
        """探索メインループを実行する。"""
        t_start = time.perf_counter()

        # テーブル初期化・セッション登録
        self._store.init_tables()
        self._store.create_session(self._config)

        logger.info(
            f"探索開始: session={self._config.session_id}, "
            f"{self._config.n_trials}トライアル, "
            f"期間={self._config.date_from}~{self._config.date_to}"
        )
        logger.info(
            f"探索空間: {self._space.total_combinations:,}通り "
            f"(ランダム{self._config.n_trials}サンプル)"
        )

        # レースデータ事前ロード
        logger.info("レースデータ事前ロード中...")
        preloaded = self._runner._load_races(
            self._config.date_from, self._config.date_to,
        )
        logger.info(f"レースデータ: {len(preloaded)}レース")

        if not preloaded:
            logger.error("レースデータが見つかりません")
            self._store.update_session_status(
                self._config.session_id, "COMPLETED", elapsed=0,
            )
            return self._reporter.generate(self._config.session_id)

        # メインループ
        best_score = 0.0
        for i in range(self._config.n_trials):
            trial_config = self._space.sample(self._rng)

            # 実行
            result = self._runner.run(
                trial_config, self._config, preloaded_races=preloaded,
            )

            # 保存
            self._store.save_trial(self._config.session_id, result)

            # 最高スコア更新
            if result.composite_score > best_score:
                best_score = result.composite_score

            # 進捗ログ
            elapsed = time.perf_counter() - t_start
            avg_per_trial = elapsed / (i + 1)
            remaining = avg_per_trial * (self._config.n_trials - i - 1)

            if (i + 1) % 10 == 0 or i == 0:
                status = "OK" if not result.error else f"ERR:{result.error[:30]}"
                logger.info(
                    f"[{i + 1}/{self._config.n_trials}] "
                    f"score={result.composite_score:.1f} "
                    f"ROI={result.roi:+.1%} "
                    f"bets={result.total_bets} "
                    f"best={best_score:.1f} "
                    f"({status}) "
                    f"ETA={remaining / 60:.0f}min"
                )

        # 完了
        total_elapsed = time.perf_counter() - t_start
        top = self._store.get_top_trials(self._config.session_id, limit=1)
        best_id = top[0]["trial_id"] if top else ""
        self._store.update_session_status(
            self._config.session_id, "COMPLETED",
            best_trial_id=best_id, elapsed=total_elapsed,
        )

        summary = self._reporter.generate(self._config.session_id)
        report = self._reporter.format_report(summary)
        logger.info(f"\n{report}")

        return summary

    def resume(self, session_id: str) -> SearchSummary:
        """中断されたセッションを再開する。"""
        session = self._store.get_session(session_id)
        if not session:
            raise ValueError(f"セッション {session_id} が見つかりません")

        completed = self._store.get_completed_count(session_id)
        remaining = self._config.n_trials - completed

        if remaining <= 0:
            logger.info(f"セッション {session_id} は既に完了しています")
            return self._reporter.generate(session_id)

        logger.info(
            f"セッション再開: {session_id}, "
            f"完了済み={completed}, 残り={remaining}"
        )

        # 乱数状態を進める（完了済み分をスキップ）
        for _ in range(completed):
            self._space.sample(self._rng)

        # レースデータ事前ロード
        preloaded = self._runner._load_races(
            self._config.date_from, self._config.date_to,
        )

        t_start = time.perf_counter()

        for i in range(remaining):
            trial_config = self._space.sample(self._rng)
            result = self._runner.run(
                trial_config, self._config, preloaded_races=preloaded,
            )
            self._store.save_trial(session_id, result)

            if (i + 1) % 10 == 0:
                time.perf_counter() - t_start
                logger.info(
                    f"[{completed + i + 1}/{self._config.n_trials}] "
                    f"score={result.composite_score:.1f} "
                    f"ROI={result.roi:+.1%}"
                )

        total_elapsed = time.perf_counter() - t_start
        top = self._store.get_top_trials(session_id, limit=1)
        best_id = top[0]["trial_id"] if top else ""
        self._store.update_session_status(
            session_id, "COMPLETED",
            best_trial_id=best_id, elapsed=total_elapsed,
        )

        summary = self._reporter.generate(session_id)
        report = self._reporter.format_report(summary)
        logger.info(f"\n{report}")
        return summary
