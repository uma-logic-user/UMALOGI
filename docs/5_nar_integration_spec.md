# UMALOGI 地方競馬（NAR）統合アーキテクチャ設計書

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-24 | 初版作成。Providerパターンによる JRA/NAR 共通コア設計を策定。|
| 2026-06-03 | NAR データ取得基盤を専用パッケージ `src/nar/` に実装（プロトタイプ）。`NarDataFetcher` 抽象 + `DummyNarFetcher`（決定的ダミー）+ `NetkeibaNarFetcher`（URL契約確定・ライブパースは明示スタブ）。Note/X 共通化のため `src/nar/note_adapter.py` を新設し、既存 `money_management.allocate_budget` / `note_generator.generate_note_draft` を `NoteBet` 互換で再利用。`feature/nar-support` ブランチで JRA 本番から完全隔離。テスト15件 PASS。影響ファイル: src/nar/__init__.py, src/nar/data_fetcher.py, src/nar/note_adapter.py, tests/test_nar_data_fetcher.py, tests/test_nar_note_adapter.py |
| 2026-06-03 | `NetkeibaNarFetcher` の出馬表/オッズ **ライブパーサを実装**（`requests`+`BeautifulSoup`・EUC-JP確定・timeout 10s・リクエスト間 1.0s ウェイト・DOM欠損/通信失敗時の安全スキップ）。純関数 `parse_shutuba_meta`/`parse_shutuba_entries`/`parse_shutuba_odds` を追加し、`http_get` 注入でモック HTML テスト可能化。実通信スモークテスト追加。ライブE2Eデモ `scripts/nar_live_demo.py` 新設（実データ→EV比例予算配分付き Note Markdown 生成）。NAR テスト 15→21 件 PASS、全体 1188 passed。影響ファイル: src/nar/data_fetcher.py, scripts/nar_live_demo.py, tests/test_nar_data_fetcher.py |
| 2026-06-04 | `src/data/race_data_provider.py` 新設。`RaceDataProvider(datasource='jra'\|'nar')` で取得経路を分岐（NAR=`NetkeibaNarFetcher` / JRA=既存 DB 読み取り）。全取得結果に `datasource` 列を強制付与、`assert_single_datasource()` で JRA/NAR 混在をガード、`provider_for_race()` で会場コード自動判定。`nar_fetcher`/`db_path` 注入でテスト可能。プロバイダテスト 13 件追加（NAR モック正常系 + JRA in-memory DB + ガード）、全体 1209 passed。影響ファイル: src/data/race_data_provider.py, tests/test_race_data_provider.py |
| 2026-06-04 | `NetkeibaNarFetcher.fetch_results` の **結果ページ（result）パーサを実装**。DTO 拡張: `NarResultRow`（着順+馬番+馬名）/ `NarPayout`（券種+組合せ+払戻金）追加、`NarRaceResult` を ranking+results+payouts に拡張。純関数 `parse_result_rows`/`parse_result_payouts`/`parse_result_page` を追加。単勝/複勝/枠連/馬連/ワイド/馬単/三連複/三連単を抽出、複勝・ワイド等の複数払戻を組合せごとに分解、カンマ・"円" を除去して int 化（`html.parser` の `<br/>` 入れ子化に依存しない正規表現方式）。DOM欠損/通信失敗時は空 DTO。NAR テスト 21→29 件 PASS、全体 1196 passed。影響ファイル: src/nar/data_fetcher.py, tests/test_nar_data_fetcher.py |

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

---

## 10. 実装ステータス（2026-06-03 / `feature/nar-support`）

本設計書の §3 Providerパターンに基づき、**NAR データ取得層と Note/X 共通化の基盤**を
専用パッケージ `src/nar/` に **JRA 本番から完全隔離して** 実装した（プロトタイプ）。

### 10.1 実装済みモジュール

| ファイル | 役割 | 状態 |
|---|---|---|
| `src/nar/__init__.py` | パッケージ宣言（隔離原則の明文化） | ✅ |
| `src/nar/data_fetcher.py` | NAR データ取得（出馬表/オッズ/結果/払戻ライブ） | ✅ |
| `src/nar/note_adapter.py` | 既存 Note/X 生成への橋渡しアダプタ | ✅ |
| `src/data/race_data_provider.py` | JRA/NAR 統合プロバイダ（datasource 切替＋混在ガード） | ✅ |
| `tests/test_nar_data_fetcher.py` | 取得基盤テスト（23件） | ✅ PASS |
| `tests/test_nar_note_adapter.py` | アダプタ・互換性テスト（6件） | ✅ PASS |
| `tests/test_race_data_provider.py` | プロバイダ・datasource 分離テスト（13件） | ✅ PASS |

### 10.2 `data_fetcher.py` の構成

- **NAR 会場マスタ** `NAR_VENUES`（門別/盛岡/水沢/浦和/船橋/大井/川崎/金沢/笠松/名古屋/園田/姫路/高知/佐賀/帯広）と
  `is_nar_race_id()`（会場コードによる JRA/NAR 判別）。
- **DTO**: `NarRaceMeta` / `NarHorseEntry` / `NarResultRow`（着順+馬番+馬名）/ `NarPayout`（券種+組合せ+払戻金）/
  `NarRaceResult`（ranking + results + payouts）。ナイター発走時刻・ダート前提など NAR 固有差分を保持。
- **抽象**: `NarDataFetcher`（`fetch_race_meta` / `fetch_entries` / `fetch_odds` / `fetch_results`）。
  本設計書 §3.1 の `RaceDataProvider` の NAR 側具象に相当する（将来 `base_provider.py` 統合時に吸収可能）。
- **`DummyNarFetcher`**: `race_id` から決定的にダミー NAR データを生成（ネットワーク不要・再現可能）。開発・テスト・E2E 雛形用。
- **`NetkeibaNarFetcher`**: `nar.netkeiba.com` を一次ソースとする取得器（**ライブ実装済み 2026-06-03**）。
  出馬表ページ（`/race/shutuba.html`）を `requests` + `BeautifulSoup` で取得し、
  `parse_shutuba_meta` / `parse_shutuba_entries` / `parse_shutuba_odds`（純関数・テスト可能）で
  `NarRaceMeta` / `NarHorseEntry` / 馬番→単勝オッズ にマッピングする。
  - **マナー/堅牢性**: HTTP `timeout=10s`、リクエスト間 `time.sleep(1.0s)`、`User-Agent` 付与。
    通信失敗・DOM 欠損時は例外で停止せず WARNING ログを出して空/既定値を返す。
  - **エンコーディング**: netkeiba は EUC-JP。`_resolve_encoding()` が Content-Type 優先・
    mac/greek 誤検知時 euc-jp フォールバック（CLAUDE.md §16 準拠）で文字化けを防ぐ。
  - **テスト容易性**: コンストラクタに `http_get` を注入してモック HTML で検証可能（ネットワーク非依存）。
  - **オッズ取得**: 単独オッズページは JS 描画のため、確実に取れる出馬表埋め込みオッズ（直前値）を一次とする。
  - **結果取得（ライブ実装済み 2026-06-04）**: 結果ページ（`/race/result.html`）を
    `parse_result_rows`（着順テーブル `table.RaceTable01` → 着順/馬番/馬名・枠番ではなく馬番側を採用）と
    `parse_result_payouts`（払戻テーブル `table.Payout_Detail_Table` → 単勝/複勝/枠連/馬連/ワイド/馬単/三連複/三連単）で解析し、
    `parse_result_page` が `NarRaceResult` を組み立てる。
    - **複数払戻**: 複勝（3 値）・ワイド（3 組）等は組合せごとに 1 つの `NarPayout` へ分解。
      組合せは `<ul>` 単位（馬連系）/ `<span>` 単位（単複系）で抽出し、払戻金は位置整合で zip。
    - **クレンジング**: 払戻金は `<数字（カンマ可）>円` を正規表現で全件抽出し、カンマ・"円" を除去して `int` 化
      （例 "1,320円"→1320）。`html.parser` の `<br/>` 入れ子化癖に依存しない堅牢方式。
    - 通信失敗・DOM 欠損時は空の `NarRaceResult` を返す（停止しない）。
- **ライブ E2E デモ**: `scripts/nar_live_demo.py` が本日の NAR 開催から実データを取得し、
  `generate_nar_note_markdown()` で EV 比例予算配分付き Note Markdown を生成する（`outputs/nar/` へ出力）。

### 10.3 `note_adapter.py`（共通化）— 互換性の核

NAR の買い目 `NarBet(bet_type, horse_desc, ev, venue)` を既存基盤の `NoteBet` 互換へ変換し、
**中央競馬で作り込んだ資産をそのまま再利用** する。

```
NarBet → to_note_bets() → NoteBet
                            ↓
        既存 money_management.allocate_budget()（EV 比例の予算配分・保険枠・100円単位保証）
                            ↓
        既存 note_generator.generate_note_draft()（🔒 有料ライン挿入付き Markdown）
                            ↓
        generate_nar_note_markdown()（NAR 文脈ヘッダーを前置）/ write_nar_drafts()（note.md + x.txt）
```

- `to_note_bet()` / `to_note_bets()`: NarBet ⇄ NoteBet 変換（順序・件数保持）。
- `generate_nar_note_markdown()`: 既存 `allocate_budget` + `generate_note_draft` を再利用し、地方競馬/会場の文脈ヘッダーを付与。
- `generate_nar_x_promo()`: NAR 向け X 集客文（≤140字保証・`#地方競馬` タグ）。
- `write_nar_drafts()`: 既存 `write_daily_drafts` と同一入出力契約で `nar_note_pre_*.md` / `nar_x_pre_*.txt` を出力。

### 10.4 安全性・隔離の担保

- 既存 `src/ops/` `src/ml/` `src/scraper/` は **1 ファイルも変更していない**（読み取り再利用のみ）。
- DB・実弾投票・bet_policy へ副作用なし（表示/取得の基盤のみ）。
- 検証: 新規 NAR テスト **29 件 PASS**（出馬表/オッズ +6・結果パーサ +8・モック HTML 注入 + 実通信スモーク graceful skip）、
  サブスク用 SNS サブセット **73 件 PASS**、全体 **1196 passed**（pre-existing 4 failures は `LIVE_MODELS` 変更起因で NAR 無関係）。

### 10.5 未実装・次フェーズ（Backlog）

1. ✅ **完了（2026-06-03〜04）**: `NetkeibaNarFetcher` の出馬表/オッズ **および確定結果（着順+払戻）** ライブパーサ実装。
   → NAR データ取得層（出馬表・オッズ・結果・払戻）は揃った。
2. 🟡 **配線完了（2026-06-04）**: `src/data/race_data_provider.py` の `RaceDataProvider`
   （`datasource='jra'|'nar'` 切替）を実装。NAR は `NetkeibaNarFetcher`、JRA は既存 DB
   読み取りへ分岐し、出馬表/結果/払戻を `datasource` 列付き正規化 DataFrame で返す。
   `assert_single_datasource()` で混在ガード、`provider_for_race()` で会場コード自動判定。
   残: DB 物理スキーマへの `datasource` 永続化（共通保存層・`races.datasource` 等）。
3. NAR 用予想モデル（特徴量カバレッジ確認・`TakeoutRates` の地方率対応）。
4. NAR 専用スケジューラ（毎日開催・ナイター対応）。

### 10.6 `race_data_provider.py`（JRA/NAR 統合）

```
RaceDataProvider(datasource="jra"|"nar", *, nar_fetcher=, db_path=)
  ├─ get_entries(race_id)  → 正規化 DataFrame（ENTRY_COLUMNS・datasource 列付き）
  ├─ get_results(race_id)  → 正規化 DataFrame（RESULT_COLUMNS）
  └─ get_payouts(race_id)  → 正規化 DataFrame（PAYOUT_COLUMNS）
       datasource="nar" → NetkeibaNarFetcher（注入可）
       datasource="jra" → 既存 DB（entries/realtime_odds/race_results/race_payouts）読み取り

provider_for_race(race_id)  … is_nar_race_id() で datasource を自動判定して生成
assert_single_datasource(df, expected=)  … JRA/NAR 混在を検知して ValueError（保存/特徴量前ガード）
```

- すべての取得結果に `datasource`（'jra'|'nar'）列を **強制付与**（`_finalize` で上書き）し、
  `assert_single_datasource` を内部適用。これにより DataFrame 段階で両データの混入を構造的に防ぐ。
- JRA 経路は **既存 JRA コード・DB を一切変更せず** 読み取りのみ（JVLink/netkeiba が取り込んだ正本を参照）。
- `nar_fetcher` / `db_path` を注入できるため、ネットワーク・実 DB 非依存でテスト可能。
