# UMALOGI 地方競馬（NAR）統合アーキテクチャ設計書

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-24 | 初版作成。Providerパターンによる JRA/NAR 共通コア設計を策定。|

---

## 1. 設計目的

現行の JRA 専用 AI 予想エンジンを、**共通コア（`src/ml/`）を一切変更せずに**
地方競馬（NAR）へ拡張する。

地方競馬は**年365日・全国16場**で毎日開催されており、
JRA（土日のみ）と比較して3〜5倍のレース機会を持つ。
この機会を取りこぼさないために、データ取得層のみを差し替える「Providerパターン」を採用する。

---

## 2. 現行アーキテクチャの整理

```
現行（JRA専用）

  [JVLink COM]          [netkeiba scraper]
  jravan_client.py  ←→  entry_table.py (fallback)
         ↓
  [SQLite: umalogi.db]
         ↓
  [FeatureBuilder]     ← src/ml/features.py
         ↓
  [LightGBM モデル]    ← src/ml/models.py / models_v2.py
         ↓
  [BetGenerator]       ← src/ml/bet_generator.py
         ↓
  [NotificationRouter] ← src/notification/router.py
```

**共通コア（変更禁止）:**
- `src/ml/features.py` — FeatureBuilder（SQLite DB → 特徴量 DataFrame）
- `src/ml/models.py` / `models_v2.py` — 本命/卍/ALPHA モデル定義・FEATURE_COLS
- `src/ml/bet_generator.py` — BetGenerator（EV計算・Kelly基準）
- `src/ml/ev_features.py` — EV特化特徴量エンジン
- `src/ml/u_score.py` — U score 18因子

---

## 3. Providerパターン設計

### 3.1 抽象インターフェース

**新規作成ファイル: `src/scraper/base_provider.py`**

```python
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Literal

RaceSource = Literal["jra", "nar"]

@dataclass
class ProviderRaceEntry:
    """プロバイダー非依存の出走馬情報 DTO"""
    race_id:        str
    race_name:      str
    race_date:      date
    venue:          str
    race_number:    int
    surface:        str          # "芝" / "ダ" / "障"
    distance:       int
    grade:          str          # "G1"/"A1"/"B" 等（ソース依存）
    source:         RaceSource   # "jra" | "nar"

    horse_number:   int
    horse_name:     str
    jockey:         str
    trainer:        str | None
    weight_carried: float
    win_odds:       float | None

@dataclass
class ProviderRaceResult:
    """確定結果 DTO"""
    race_id:     str
    horse_number: int
    rank:        int | None
    finish_time: str | None
    margin:      str | None

@dataclass
class ProviderPayout:
    """払戻 DTO"""
    race_id:     str
    bet_type:    str
    combination: str
    payout:      int


class RaceDataProvider(ABC):
    """JRA / NAR 共通データプロバイダー抽象クラス"""

    source: RaceSource  # サブクラスで定義

    @abstractmethod
    def fetch_race_ids(self, target_date: date) -> list[str]:
        """指定日の全 race_id を返す"""

    @abstractmethod
    def fetch_entries(self, race_id: str) -> list[ProviderRaceEntry]:
        """出走馬一覧を返す（オッズ含む）"""

    @abstractmethod
    def fetch_odds(self, race_id: str) -> dict[int, float]:
        """horse_number → 単勝オッズ マップを返す（リアルタイム or 直前）"""

    @abstractmethod
    def fetch_results(self, race_id: str) -> list[ProviderRaceResult]:
        """確定着順を返す（レース後）"""

    @abstractmethod
    def fetch_payouts(self, race_id: str) -> list[ProviderPayout]:
        """確定払戻を返す（レース後）"""

    def save_to_db(self, con: sqlite3.Connection, race_id: str) -> None:
        """
        エントリー → races / race_results テーブルへ保存する共通ロジック。
        各テーブルに datasource='jra'|'nar' カラムを追加することでJRA/NARを識別。
        """
        entries = self.fetch_entries(race_id)
        # 実装は src/database/init_db.py の insert_race_entry() を呼ぶ
        ...
```

### 3.2 JRA プロバイダー（既存コードのラッパー）

**新規作成ファイル: `src/scraper/jra_provider.py`**

```python
from src.scraper.jravan_client import JVLinkClient
from src.scraper.entry_table import fetch_entries_from_netkeiba
from src.scraper.base_provider import RaceDataProvider, ProviderRaceEntry, RaceSource

class JRADataProvider(RaceDataProvider):
    """JVLink（一次）+ netkeiba（フォールバック）を使った JRA プロバイダー"""

    source: RaceSource = "jra"

    def __init__(self, jvlink_timeout: int = 10) -> None:
        self._jvlink = JVLinkClient(timeout=jvlink_timeout)

    def fetch_race_ids(self, target_date: date) -> list[str]:
        try:
            return self._jvlink.get_race_ids(target_date)
        except Exception:
            # JVLink 失敗時は netkeiba から取得
            return fetch_race_ids_from_netkeiba(target_date)

    def fetch_entries(self, race_id: str) -> list[ProviderRaceEntry]:
        try:
            raw = self._jvlink.get_entries(race_id)
            return [_map_jvlink_entry(e, race_id) for e in raw]
        except Exception:
            raw = fetch_entries_from_netkeiba(race_id)
            return [_map_netkeiba_entry(e, race_id) for e in raw]

    # ... fetch_odds / fetch_results / fetch_payouts も同様
```

### 3.3 NAR プロバイダー（新規実装）

**新規作成ファイル: `src/scraper/nar_provider.py`**

地方競馬データソース候補（優先順）:

| 優先 | ソース | 方式 | 備考 |
|-----|-------|------|------|
| 1位 | **NAR DATA Gateway** (keibadata.or.jp) | API / ファイル取得 | 有料。確定データ信頼性最高 |
| 2位 | **netkeiba.com/local/race/** | HTML スクレイピング | 無料。構造変更リスクあり |
| 3位 | **SPAT4 / 楽天競馬 / オッズパーク** | スクレイピング | オッズ取得に特化 |

```python
import requests
from bs4 import BeautifulSoup
from src.scraper.base_provider import RaceDataProvider, ProviderRaceEntry, RaceSource

class NARDataProvider(RaceDataProvider):
    """
    地方競馬（NAR）データプロバイダー。
    Phase 1: netkeiba /local/ スクレイピングで実装。
    Phase 2: NAR DATA Gateway API（有料）へ移行予定。
    """

    source: RaceSource = "nar"
    _BASE_URL = "https://nar.netkeiba.com"

    def fetch_race_ids(self, target_date: date) -> list[str]:
        """
        netkeiba NAR の開催一覧ページから race_id を取得する。
        race_id 形式: "2026052410001" (YYYYMMDD + 場コード + R番号) ← JRA と異なる
        """
        url = f"{self._BASE_URL}/top/race_list_sub.html?kaisai_date={target_date.strftime('%Y%m%d')}"
        # ... スクレイピング実装

    def fetch_entries(self, race_id: str) -> list[ProviderRaceEntry]:
        url = f"{self._BASE_URL}/race/shutuba.html?race_id={race_id}"
        # ... スクレイピング実装

    # 地方競馬固有の注意点:
    # - race_id フォーマットが JRA と異なる（DB に datasource カラムで識別）
    # - 斤量は JRA より軽い（NAR は 54kg 基準）
    # - グレード: A1/A2/B/C（JRA の G1/G2/G3/OP と異なる）
    # - 1日最大12R × 16場 = 192レース/日
```

---

## 4. DBスキーマ拡張

現行 `races` / `race_results` テーブルに以下のカラムを追加する（マイグレーション必要）:

```sql
-- races テーブル拡張
ALTER TABLE races ADD COLUMN datasource TEXT NOT NULL DEFAULT 'jra';
  -- 'jra' | 'nar'

ALTER TABLE races ADD COLUMN region TEXT;
  -- 'central' (JRA) | 'south_kanto' | 'hokkaido' | 'tokai' 等 (NAR)

ALTER TABLE races ADD COLUMN grade TEXT;
  -- JRA: 'G1'/'G2'/'G3'/'OP'/'3勝'/'2勝'/'1勝'/'未勝利'/'新馬'
  -- NAR: 'A1'/'A2'/'B'/'C'/'D'/'重賞'

-- 既存インデックスに datasource を追加
CREATE INDEX IF NOT EXISTS idx_races_datasource_date
  ON races (datasource, date);
```

マイグレーション実装場所: `src/database/init_db.py` の `_migrate_nar_support()` 関数として追加。

---

## 5. 特徴量アダプター設計

`src/ml/features.py` の `FeatureBuilder` は SQLite DB から特徴量を生成するため、
**DBが統一されている限り JRA/NAR の違いを意識しない**。

ただし以下の NAR 固有特徴量を追加する:

**新規追加ファイル: `src/ml/nar_features.py`**

```python
class NARFeatureAdapter:
    """
    NAR 固有特徴量を追加する FeatureBuilder アダプター。
    FeatureBuilder.build_race_features() の結果に対して後処理として適用する。
    """

    NAR_FEATURE_COLS: list[str] = [
        "nar_jockey_win_rate",    # 地方騎手の当該場勝率
        "nar_horse_distance_win_rate",  # 同距離過去勝率（NAR）
        "nar_grade_rank",         # A1=4 / A2=3 / B=2 / C=1 / D=0
        "nar_field_size_ratio",   # 出走頭数 / 8（地方は小頭数多い）
        "nar_days_since_last_run", # 前走間隔（NAR は中2日等短間隔多い）
    ]

    def add_nar_features(self, df: pd.DataFrame, con: sqlite3.Connection) -> pd.DataFrame:
        """
        JRA 共通特徴量 DataFrame に NAR 固有特徴量を追加して返す。
        モデルの FEATURE_COLS に NAR_FEATURE_COLS を含む場合のみ有効。
        """
        ...
```

**モデルの対応:**
- JRA専用モデル: 既存 `FEATURE_COLS`（69列）をそのまま使用
- NAR専用モデル: `FEATURE_COLS + NARFeatureAdapter.NAR_FEATURE_COLS`（74列程度）
- JRA/NAR 統合モデル: datasource フラグを特徴量として追加（将来検討）

---

## 6. パイプライン統合設計

`src/pipeline/prediction.py` の `prerace_pipeline()` を **provider 引数化**する:

```python
def prerace_pipeline(
    race_id: str,
    provider: RaceDataProvider | None = None,
    *,
    con: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """
    provider を指定しない場合は自動的に datasource='jra' の JRADataProvider を使用。
    NAR レースは NARDataProvider を渡して呼ぶ。

    Usage:
        # JRA（既存の呼び出しと互換）
        result = prerace_pipeline("202605021201")

        # NAR（新規）
        from src.scraper.nar_provider import NARDataProvider
        nar = NARDataProvider()
        result = prerace_pipeline("2026052410001", provider=nar)
    """
    if provider is None:
        provider = JRADataProvider()   # 後方互換
    ...
```

**`scripts/scheduler.py` への統合:**

```python
# NAR 開催日の直前予想ループ（既存 today_auto_runner の NAR 版）
def today_auto_runner_nar(target_date: date) -> None:
    from src.scraper.nar_provider import NARDataProvider
    provider = NARDataProvider()
    race_ids = provider.fetch_race_ids(target_date)
    for race_id in race_ids:
        prerace_pipeline(race_id, provider=provider)
```

スケジューラへの追加登録（将来）:
```python
# 毎日 08:30 に JRA + NAR の両方を起動
schedule.every().day.at("08:30").do(today_auto_runner_nar)   # NAR追加
```

---

## 7. 実装フェーズ計画

| フェーズ | 内容 | 期間目安 |
|---------|------|---------|
| **Phase N-0** | 本ドキュメント確定・DBスキーマ拡張マイグレーション | 1週間 |
| **Phase N-1** | `base_provider.py` / `jra_provider.py`（既存コードのラッパー化） | 2週間 |
| **Phase N-2** | `nar_provider.py` 実装（netkeiba/local スクレイピング） | 3週間 |
| **Phase N-3** | `nar_features.py` 実装 + NAR向けモデル訓練 | 2週間 |
| **Phase N-4** | scheduler.py 統合 + バックテスト（過去1年分NAR） | 2週間 |
| **Phase N-5** | 本番稼働開始（南関東・北海道・名古屋を優先） | — |

---

## 8. データソース候補の詳細調査メモ

### 8.1 NAR DATA Gateway（優先度：高）

- URL: https://keibadata.or.jp/
- 提供: NAR（地方競馬全国協会）公式
- 取得方式: ファイル形式（JVLink の地方版的な仕様）
- コスト: 要確認（JRA-VANと同様の有料SID制）
- 利用規約: スクレイピング禁止のため公式APIが必要

### 8.2 netkeiba.com /nar/ スクレイピング（優先度：中）

- URL: https://nar.netkeiba.com/
- 利用規約: 個人利用の範囲で調査必要
- 取得可能データ: 出走表・オッズ・確定結果・払戻
- リスク: HTML構造変更による突然の破損

### 8.3 SPAT4 / 楽天競馬（優先度：低）

- オッズのみ特化取得に利用可能
- 会員登録が必要な場合あり

---

## 9. 制約・注意事項

1. **地方競馬の race_id 形式**: JRA の `YYYYKKNNVVRR` 形式と異なる。
   DB の `races.race_id` カラムは現在 JRA 形式を前提とするため、
   NAR 追加時は `races.datasource` カラムで識別すること。

2. **地方競馬の払戻税率**: JRA（25%）と地方（25% 同等）だが、
   場によって差がある可能性があるため `ev_features.py` の
   `JRATakeoutRates` を `TakeoutRates` に改名して地方率を追加する。

3. **週末凍結ルール（CLAUDE.md 条項2）**: NAR は毎日開催のため、
   土日の稼働（レース取得・Hit Flash）は継続。「改修の凍結」であり稼働停止ではない。

4. **JVLink との分離**: NAR は JVLink COM（32bit専用）を使わない。
   64bit Python で直接スクレイピング可能なため、32bit/64bitの分離設計が不要。
   スケジューラの複雑度が JRA より低くなる。
