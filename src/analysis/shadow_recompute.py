"""
src/analysis/shadow_recompute.py — 5月末ポリシー並列シャドー再計算エンジン (v1.15.0-dev)

【目的】
2026-06-02 の「単複ロック」導入以前（5月末稼働期）のベットポリシー＝全 7 券種を
出力していた挙動を、**現行モデルスコア・現行データハイジーン**(races.grade 列・破損行の
status='error' 隔離) に適用して、別名 model_id ``{base}_v0525`` として並列復活させる。

【絶対遵守する分離原則（CLAUDE.md 条項1）】
  - 本エンジンは live パイプライン (_prerace_pipeline_inner) を一切変更しない。
  - 読み取り専用スコアリング → 生成 → 隔離 model_id への INSERT のみ。
  - シャドー model_type (``卍_v0525`` 等) は live の ``卍(直前)`` 等とは別キーであり、
    predictions UNIQUE(race_id, model_type, bet_type) 上で衝突しない。既存 live 予想を
    UPDATE/REPLACE することは構造上発生しない（条項1 を自動担保）。

【「5月末ロジック」の正体と出典（git 走査結果）】
  EV 算出アルゴリズム（Harville 公式 + OddsEstimator + 本命/卍 モデル）は 5月末から
  実質不変であり、差分は **ポリシー層**（どの券種を出力するか）に存在する。
    550ded89  本命三連単 条件付き開放（個別 EV>=1.5）
    5e854489  卍三連複 EV>=1.0 ゲート
    02dc564b  ROI フィルター + 動的 Kelly
    800aa23f  (2026-06-02) 単複ロック導入 → 多券種を実弾出力から除外
  すなわち単複ロック以前は本命/卍とも単勝〜三連単の全券種を出力していた。本エンジンは
  生の ManjiStrategy / HonmeiStrategy（= 5月末から不変の全券種ジェネレーター）を
  単複ロックを経由せず直接呼び、全 7 券種を ``_v0525`` として復元する。

【収益トラック分離（条項3 相当）】
  各モデルは ``{base}_v0525`` の一意な model_type で記録され、live トラックと混ざらず
  個別に回収率・期待値を追跡できる。
"""

from __future__ import annotations

import json as _json
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.ml.bet_generator import (
    HitFocusStrategy,
    HonmeiStrategy,
    ManjiStrategy,
    OddsEstimator,
    RaceBets,
    VirtualOracleStrategy,
)

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

# シャドー model_id サフィックス（5月末ポリシー復活枠）
SHADOW_SUFFIX: str = "_v0525"
# model_type に付与する再計算タグ（base_model 正規表現の (直前|暫定) には該当せず素通り）
RECOMPUTE_TAG: str = "(再計算)"

# 多券種フルデッキを生成する 4 モデル（本命/卍 は単勝〜三連単、Oracle/HitFocus は多券種）。
# これらが「単複ロックで消えた」馬連・馬単・ワイド・三連複・三連単を完全復元する。
MULTI_DECK_BASES: tuple[str, ...] = ("本命", "卍", "Oracle", "HitFocus")
# 単複特化のシャドートラック（5月末も単複のみ）。収益トラック分離のため別 model_id で保持。
SINGLE_PLACE_BASES: tuple[str, ...] = ("Pure_EV_Edge", "FukushoElite", "Alpha-Payout")

# insert_prediction の許可ベースへ追加すべき全シャドーベース。
SHADOW_BASES: tuple[str, ...] = tuple(
    f"{b}{SHADOW_SUFFIX}" for b in (*MULTI_DECK_BASES, *SINGLE_PLACE_BASES)
)


def shadow_model_type(base: str) -> str:
    """live ベース名から隔離シャドー model_type を構築する。

    例: ``本命`` → ``本命_v0525(再計算)`` / ``卍`` → ``卍_v0525(再計算)``。
    """
    return f"{base}{SHADOW_SUFFIX}{RECOMPUTE_TAG}"


def shadow_base(base: str) -> str:
    """``本命`` → ``本命_v0525`` （タグ無しの収益トラックキー）。"""
    return f"{base}{SHADOW_SUFFIX}"


@dataclass
class ShadowRaceResult:
    """1 レース分のシャドー生成結果。"""

    race_id: str
    # {shadow_base: RaceBets}（例 "本命_v0525" -> RaceBets）
    model_bets: dict[str, RaceBets] = field(default_factory=dict)
    skipped_reason: str | None = None

    @property
    def total_bets(self) -> int:
        return sum(len(rb.bets) for rb in self.model_bets.values())


def generate_shadow_bets(
    race_id: str,
    df: "pd.DataFrame",
    honmei_scores: "pd.Series",
    ev_scores: "pd.Series",
    *,
    estimator: OddsEstimator | None = None,
    bankroll: float = 100_000.0,
) -> dict[str, RaceBets]:
    """現行モデルスコアに 5月末ポリシー（全券種出力）を適用してシャドー買い目を生成する。

    DB 書き込みは行わない純粋関数（テスト容易性のためスコアを注入）。
    生の ManjiStrategy / HonmeiStrategy を直接呼ぶため単複ロックを経由せず全 7 券種が出る。

    Args:
        race_id:       レース ID。
        df:            FeatureBuilder.build_race_features() の出力。
        honmei_scores: HonmeiModel.predict(df) の出力（勝率スコア）。
        ev_scores:     ManjiModel.ev_score(df) の出力（EV 比率）。
        estimator:     券種別払戻スケール推定器（None なら既定スケール）。
        bankroll:      Kelly 計算用残高。

    Returns:
        {shadow_base: RaceBets}。例 {"本命_v0525": RaceBets, "卍_v0525": RaceBets, ...}。
    """
    est = estimator or OddsEstimator()
    out: dict[str, RaceBets] = {}

    # 本命_v0525 / 卍_v0525: 単勝〜三連単のフルデッキ（単複ロック非経由）
    honmei = HonmeiStrategy(estimator=est).generate(
        race_id, df, honmei_scores, bankroll=bankroll
    )
    out[shadow_base("本命")] = honmei
    manji = ManjiStrategy(estimator=est).generate(
        race_id, df, ev_scores, bankroll=bankroll
    )
    out[shadow_base("卍")] = manji

    # Oracle_v0525: 三連複・三連単の的中確率最大買い目（本命スコア由来）
    oracle = VirtualOracleStrategy().generate(race_id, df, honmei_scores)
    out[shadow_base("Oracle")] = oracle

    # HitFocus_v0525: 2軸マルチフォーメーション（馬連/馬単/三連単）
    hit_focus = HitFocusStrategy(estimator=est).generate(race_id, df, honmei_scores)
    out[shadow_base("HitFocus")] = hit_focus

    return out


def save_shadow_bets(
    conn: sqlite3.Connection,
    race_id: str,
    model_bets: dict[str, RaceBets],
) -> dict[str, list[int]]:
    """シャドー買い目を ``{base}_v0525(再計算)`` model_type で predictions へ追記する。

    既存 live 予想は別 model_type のため一切変更されない（条項1）。同一シャドーキーの
    再実行時は INSERT OR REPLACE で自身の前回行のみ更新する。

    Returns:
        {shadow_base: [prediction_id, ...]}。
    """
    from src.database.init_db import insert_prediction

    saved: dict[str, list[int]] = {}
    for sbase, race_bets in model_bets.items():
        mt = f"{sbase}{RECOMPUTE_TAG}"
        pids: list[int] = []
        for bet in race_bets.bets:
            horses_payload: list[dict] = []
            for combo in bet.combinations[:5]:
                for j, horse_num in enumerate(combo):
                    hn = int(horse_num)
                    horses_payload.append(
                        {
                            "horse_number": hn,
                            "horse_name": (
                                bet.horse_names[j]
                                if j < len(bet.horse_names)
                                else str(hn)
                            ),
                            "predicted_rank": j + 1,
                            "model_score": bet.model_score,
                            "ev_score": bet.expected_value,
                        }
                    )
            combo_json = _json.dumps([list(c) for c in bet.combinations])
            try:
                pid = insert_prediction(
                    conn,
                    race_id=race_id,
                    model_type=mt,
                    bet_type=bet.bet_type,
                    horses=horses_payload,
                    confidence=bet.confidence,
                    expected_value=bet.expected_value,
                    recommended_bet=bet.recommended_bet,
                    notes=f"[shadow v0525] {bet.notes}",
                    combination_json=combo_json,
                )
                pids.append(pid)
            except Exception as exc:  # noqa: BLE001 — 1 件失敗で全体を止めない
                logger.error("シャドー保存失敗 %s %s: %s", mt, bet.bet_type, exc)
        saved[sbase] = pids
    return saved


def score_race(
    conn: sqlite3.Connection,
    race_id: str,
) -> tuple["pd.DataFrame", "pd.Series", "pd.Series"]:
    """DB 内データのみで特徴量を構築し本命/卍スコアを算出する（外部通信なし）。

    Returns:
        (df, honmei_scores, ev_scores)。
    """
    from src.ml.features import FeatureBuilder
    from src.ml.models import load_models

    df = FeatureBuilder(conn).build_race_features(race_id)
    if df.empty:
        raise ValueError(f"race_id={race_id}: 特徴量 DataFrame が空")
    honmei_model, _place_model, manji_model = load_models()
    honmei_scores = honmei_model.predict(df)
    ev_scores = manji_model.ev_score(df)
    return df, honmei_scores, ev_scores


def recompute_race(
    conn: sqlite3.Connection,
    race_id: str,
    *,
    estimator: OddsEstimator | None = None,
    bankroll: float = 100_000.0,
    save: bool = True,
) -> ShadowRaceResult:
    """1 レースを DB 内データで再計算し、シャドー買い目を生成（任意で保存）する。"""
    try:
        df, honmei_scores, ev_scores = score_race(conn, race_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("シャドー再計算スキップ race_id=%s: %s", race_id, exc)
        return ShadowRaceResult(race_id=race_id, skipped_reason=str(exc))

    model_bets = generate_shadow_bets(
        race_id,
        df,
        honmei_scores,
        ev_scores,
        estimator=estimator,
        bankroll=bankroll,
    )
    if save:
        save_shadow_bets(conn, race_id, model_bets)
    return ShadowRaceResult(race_id=race_id, model_bets=model_bets)


# ── 仮想 ROI サマリ ────────────────────────────────────────────────


@dataclass
class VirtualRoiRow:
    """モデル×券種ごとの仮想 ROI 集計行。"""

    model: str
    bet_type: str
    n_bets: int = 0
    invested: float = 0.0
    payout: float = 0.0

    @property
    def profit(self) -> float:
        return self.payout - self.invested

    @property
    def roi(self) -> float:
        return (self.payout / self.invested * 100.0) if self.invested > 0 else 0.0


def virtual_roi_summary(
    conn: sqlite3.Connection,
    race_ids: list[str],
) -> tuple[dict[tuple[str, str], VirtualRoiRow], int]:
    """確定結果のあるレースについて、シャドー予想のみの仮想 ROI を集計する。

    既存評価器 evaluate_race(dry_run=True) を再利用し、同着・返還を正しく処理する。
    シャドー以外（live）の予想は集計から除外する。prediction_results への書き込みは行わない。

    Returns:
        ({(model, bet_type): VirtualRoiRow}, settled_race_count)。
    """
    from src.evaluation.evaluator import Evaluator

    evaluator = Evaluator()
    rows: dict[tuple[str, str], VirtualRoiRow] = {}
    settled = 0

    for race_id in race_ids:
        # 結果が無い（未確定）レースはスキップ
        has_result = conn.execute(
            "SELECT COUNT(*) FROM race_results WHERE race_id = ? AND rank IS NOT NULL",
            (race_id,),
        ).fetchone()[0]
        if not has_result:
            continue

        # このレースのシャドー prediction_id → model_type を取得
        shadow_pid_model: dict[int, str] = {}
        for pid, mt in conn.execute(
            "SELECT id, model_type FROM predictions "
            "WHERE race_id = ? AND model_type LIKE ?",
            (race_id, f"%{SHADOW_SUFFIX}%"),
        ).fetchall():
            shadow_pid_model[int(pid)] = str(mt)
        if not shadow_pid_model:
            continue

        try:
            result = evaluator.evaluate_race(conn, race_id, dry_run=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("仮想ROI評価失敗 race_id=%s: %s", race_id, exc)
            continue

        counted = False
        for detail in result.hits:
            mt = shadow_pid_model.get(detail.prediction_id)
            if mt is None:
                continue  # live 予想は除外
            counted = True
            base = mt.split("(")[0]  # 例 "本命_v0525"
            key = (base, detail.bet_type)
            row = rows.setdefault(
                key, VirtualRoiRow(model=base, bet_type=detail.bet_type)
            )
            row.n_bets += 1
            row.invested += float(detail.invested)
            row.payout += float(detail.payout)
        if counted:
            settled += 1

    return rows, settled
