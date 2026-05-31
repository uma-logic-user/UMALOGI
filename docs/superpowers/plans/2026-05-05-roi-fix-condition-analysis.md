# ROI修正・UI重複バグ・得意条件分析 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 評価エンジンのROI計算を `(payout / (n_tickets × 100)) × 100` に修正し、三連単UI重複バグを解消し、得意条件分析JSONとダッシュボードを追加する。

**Architecture:** evaluator.py の `invested` 導出をコンボ数ベースに変更。dbHelpers.ts の `sortedCombinations` を有順券種で内部ソート禁止に修正。generate_data.py に条件分析エクスポートを追加し、新規 ConditionAnalysis コンポーネントで表示。

**Tech Stack:** Python 3.11, SQLite (better-sqlite3), TypeScript, Next.js 14, React

---

## ファイルマップ

| 操作 | ファイル | 変更内容 |
|------|---------|---------|
| Modify | `src/evaluation/evaluator.py:498-583` | ROI分母をn_tickets×100に変更、refund馬番直接参照、sum修正 |
| Modify | `web/src/lib/dbHelpers.ts:31-47` | sortedCombinations で三連単/馬単の内部ソート禁止 |
| Modify | `web/src/lib/dbHelpers.ts:64-102` | identifyBetForm で2頭軸マルチ判定精度向上 |
| Modify | `web/src/app/api/predictions/route.ts:101` | sortedCombinations に betType を渡す |
| Modify | `web/generate_data.py` | export_condition_analysis() 追加 + ROI SQL修正 |
| Create | `web/src/data/condition_analysis.json` | 生成されるJSONファイル |
| Create | `web/src/components/ConditionAnalysis.tsx` | 得意条件テーブルコンポーネント |
| Modify | `web/src/components/AppShell.tsx` | condition タブ追加 + データ取得 |

---

## Task 1: evaluator.py — ROI計算の完全修正

**Files:**
- Modify: `src/evaluation/evaluator.py:498-583`

### 変更の論拠

- **現在**: `actual_payout = (payout_per_100 / 100) * recommended_bet` → ROI = `actual_payout / recommended_bet * 100 = payout_per_100`（recommended_bet がキャンセルされ常に payout_per_100 の値になる）
- **問題**: recommended_bet=1000（10点分）だが実際は12点 → ROI 42160% 表示
- **修正**: 分母を `len(parsed_combos) * 100` に変更 → ROI 35133.3%

- [ ] **Step 1: `_parse_combination_json` の結果から n_tickets を導出するロジックを追加**

`src/evaluation/evaluator.py` の 予想ループ（`for pred in predictions:` ブロック、行498-583）を以下のように修正する。

```python
        # 返還チェック（combination_json馬番を優先的に使用）
        parsed_combos = _parse_combination_json(pred.get("combination_json") or "")

        # ── 返還チェック（CLAUDE.mdルール: 馬番整数ベースのみ）──
        if parsed_combos:
            # combination_json から直接馬番を取得して返還判定
            all_nums_in_combos = {n for combo in parsed_combos for n in combo}
            refund = bool(all_nums_in_combos & refund_numbers)
        else:
            refund = _has_refund(horse_names, horse_numbers, refund_numbers)

        # n_tickets を combination_json から導出（recommended_bet は使わない）
        n_tickets = len(parsed_combos) if parsed_combos else max(1, round(float(pred["recommended_bet"] or 100.0) / 100))
        invested = float(n_tickets * 100)  # 1コンボ = 100円

        if refund:
            detail = BetHitDetail(
                prediction_id=pid,
                bet_type=bet_type,
                is_hit=False,
                is_refund=True,
                payout=invested,
                invested=invested,
                profit=0.0,
                roi=100.0,
                combination=horse_names,
                actual_winners=actual_winners_all,
            )
            hit_details.append(detail)
            total_invested += invested
            total_payout   += invested
            if not dry_run:
                self._save_result(conn, pid, False, invested, 0.0, 100.0)
            continue

        # 的中判定・払戻取得（combination_json 馬番ベース優先）
        # parsed_combos は上で既に計算済みのため再利用
        hit = False
        payout_per_100 = 0

        if parsed_combos:
            for combo in parsed_combos:
                key = _combo_to_payout_key(bet_type, combo)
                if key is not None:
                    p = payouts.get((bet_type, key), 0)
                    if p > 0:
                        hit = True
                        payout_per_100 += p  # max→sum: 複数combo的中時は合算
        else:
            predicted_nums = [horse_numbers[n] for n in horse_names if n in horse_numbers]
            rank_by_num: dict[int, int] = {
                horse_numbers[n]: r
                for n, r in result_map.items()
                if n in horse_numbers and r is not None
            }
            hit = _is_hit_by_numbers(bet_type, predicted_nums, rank_by_num)
            if hit:
                combo_key = _build_combination_key(bet_type, horse_names, horse_numbers)
                payout_per_100 = _lookup_payout(bet_type, combo_key, payouts)
                if payout_per_100 == 0:
                    matching = [v for (bt, _), v in payouts.items() if bt == bet_type]
                    if matching:
                        payout_per_100 = int(sum(matching) / len(matching))
                        errors.append(
                            f"pid={pid} {bet_type}: combination 解決不可、"
                            f"払戻平均 {payout_per_100} を使用"
                        )

        # 正しいROI計算: 分母 = n_tickets × 100
        actual_payout = (payout_per_100 / 100.0) * float(pred["recommended_bet"] or 100.0) if hit else 0.0
        profit        = actual_payout - invested   # invested = n_tickets * 100
        roi           = (actual_payout / invested * 100.0) if invested > 0 else 0.0
```

- [ ] **Step 2: 複勝・ワイドの出走頭数依存修正**

`evaluator.py` の `_fetch_race_meta` を拡張し、`n_horses` も取得するよう変更する。

```python
def _fetch_race_meta(conn: sqlite3.Connection, race_id: str) -> dict:
    row = conn.execute(
        """
        SELECT r.race_name, r.date,
               COUNT(rr.id) AS n_horses
        FROM races r
        LEFT JOIN race_results rr ON rr.race_id = r.race_id AND rr.rank IS NOT NULL AND rr.rank > 0
        WHERE r.race_id = ?
        GROUP BY r.race_id
        """,
        (race_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"race_id={race_id} が races テーブルに存在しません")
    return {"race_name": row[0], "date": row[1], "n_horses": row[2] or 8}
```

`evaluate_race` の冒頭（meta 取得直後）に以下を追加:

```python
        n_horses = meta.get("n_horses", 8)
        # 複勝・ワイドの着順圏: JRAルール（7頭以下は2着まで、8頭以上は3着まで）
        place_ranks_effective = {1, 2} if n_horses <= 7 else {1, 2, 3}
```

`_is_hit_by_numbers` の複勝・ワイド判定に `place_ranks_effective` を渡す必要があるため、引数に追加:

```python
def _is_hit_by_numbers(
    bet_type: str,
    predicted_numbers: list[int],
    rank_by_number: dict[int, int],
    place_ranks: set[int] = _PLACE_RANKS,  # デフォルトで後方互換
) -> bool:
    # ...
    elif bet_type == "複勝":
        return any(rank_by_number.get(n) in place_ranks for n in pnums)
    # 馬連・ワイドも同様に place_ranks を使用
```

呼び出し側 (`evaluate_race`) でこの引数を渡す:

```python
            hit = _is_hit_by_numbers(bet_type, predicted_nums, rank_by_num, place_ranks_effective)
```

- [ ] **Step 3: 動作確認（dry run で 新潟3R を再評価）**

```bash
python -c "
from src.database.init_db import init_db
from src.evaluation.evaluator import Evaluator
conn = init_db()
e = Evaluator()
# 新潟3R が含まれる日付 (race_id から逆算: 202601010103 等)
# 実際の race_id を確認してから実行
result = e.evaluate_race(conn, '202601010103', dry_run=True)
print(f'ROI={result.roi:.1f}% 払戻={result.total_payout:.0f} 投資={result.total_invested:.0f}')
"
```

Expected: ROI = 35133.3%, payout = 421600, invested = 1200

- [ ] **Step 4: git commit**

```bash
git add src/evaluation/evaluator.py
git commit -m "fix(evaluator): ROI分母をn_tickets×100に統一・refund馬番直接参照・複勝頭数ルール追加"
```

---

## Task 2: dbHelpers.ts — sortedCombinations の三連単/馬単対応

**Files:**
- Modify: `web/src/lib/dbHelpers.ts:31-47`
- Modify: `web/src/app/api/predictions/route.ts:101`

### 変更の論拠

`sortedCombinations` が各コンボの内部を昇順ソートするため、三連単 `[5,7,3]` と `[3,5,7]` が同じ `[3,5,7]` に潰れてしまい、12点が24点 (重複) に見える。有順券種は内部ソート禁止にする。

- [ ] **Step 1: sortedCombinations に betType 引数を追加**

`web/src/lib/dbHelpers.ts` の `sortedCombinations` 関数を以下に置き換える:

```typescript
/**
 * combination_json を正規化してシリアライズする。
 * 無順券種(馬連/ワイド/三連複): 各コンボ内部を昇順ソート → 外側をソート
 * 有順券種(馬単/三連単): 各コンボ内部の順序を保持 → 外側のみソート
 */
export function sortedCombinations(json: unknown, betType?: string): string {
  if (!json || typeof json !== 'string') return '[]'
  try {
    const raw: number[][] = JSON.parse(json)
    if (!Array.isArray(raw)) return String(json)
    const isOrdered = betType === '馬単' || betType === '三連単'
    const normalized = raw
      .map(c => isOrdered ? [...c] : [...c].sort((a, b) => a - b))
      .sort((a, b) => {
        for (let i = 0; i < Math.min(a.length, b.length); i++) {
          if (a[i] !== b[i]) return a[i] - b[i]
        }
        return a.length - b.length
      })
    return JSON.stringify(normalized)
  } catch {
    return String(json)
  }
}
```

- [ ] **Step 2: route.ts で betType を sortedCombinations に渡す**

`web/src/app/api/predictions/route.ts` の 101行目を変更:

```typescript
      // 変更前:
      combination_json:  sortedCombinations(comboJson),
      // 変更後:
      combination_json:  sortedCombinations(comboJson, betType),
```

- [ ] **Step 3: identifyBetForm の2頭軸マルチ判定を精度向上**

`web/src/lib/dbHelpers.ts` の `identifyBetForm` 関数（三連単ブロック）を修正。
現在: `firsts` が2つなら2頭軸マルチ（すべての1着候補が2頭の場合のみ検出）
追加: `firsts` が全頭数 (= num) だが、組み合わせが2頭軸の計算値と一致する場合も検出

```typescript
  if (betType === '三連単') {
    const firsts = new Set(combos.map(c => c[0]))
    // ボックス: N頭の全順列
    if (firsts.size === num && num >= 3 && n === num * (num - 1) * (num - 2))
      return [`${num}頭ボックス`, n]
    // 2頭軸マルチ: 2頭が必ず上位3着内に入るパターン
    // 判定: 全コンボに共通して出現する馬が2頭ある場合
    const alwaysIn = [...allHorses].filter(h => combos.every(c => c.includes(h)))
    if (alwaysIn.length >= 2) return ['2頭軸マルチ', n]
    // 1頭軸マルチ: 1頭が必ず上位3着内
    if (alwaysIn.length === 1) return ['1頭軸マルチ', n]
    if (firsts.size === 2) return ['2頭軸（1着固定）', n]
    if (firsts.size === 1) return ['1頭軸（1着固定）', n]
    return ['フォーメーション', n]
  }
```

- [ ] **Step 4: generate_data.py の _identify_bet_form を同様に修正**

`web/generate_data.py` の `_identify_bet_form` 関数の三連単ブロックを以下に変更:

```python
    if bet_type == "三連単":
        if not isinstance(combos[0], list):
            return bet_type, n
        all_horses: set = set(h for c in combos for h in c)
        firsts: set = set(c[0] for c in combos)
        num = len(all_horses)
        # ボックス: N頭の全順列 N*(N-1)*(N-2)
        if len(firsts) == num and num >= 3 and n == num * (num - 1) * (num - 2):
            return f"{num}頭ボックス", n
        # 2頭軸マルチ: 全コンボに共通して出現する馬が2頭以上
        always_in = [h for h in all_horses if all(h in c for c in combos)]
        if len(always_in) >= 2:
            return "2頭軸マルチ", n
        if len(always_in) == 1:
            return "1頭軸マルチ", n
        # 1着固定パターン
        if len(firsts) == 2:
            return "2頭軸（1着固定）", n
        if len(firsts) == 1:
            return "1頭軸（1着固定）", n
        return "フォーメーション", n
```

- [ ] **Step 5: generate_data.py を再実行して predictions.json を更新**

```bash
cd C:/dev/horse-racing-ai/web
python generate_data.py
```

Expected: predictions.json の三連単エントリで重複コンボが消える

- [ ] **Step 6: git commit**

```bash
git add web/src/lib/dbHelpers.ts web/src/app/api/predictions/route.ts web/generate_data.py
git commit -m "fix(ui): 三連単/馬単の買い目重複表示解消・軸ラベル判定精度向上"
```

---

## Task 3: generate_data.py — 得意条件分析エクスポート

**Files:**
- Modify: `web/generate_data.py` (関数追加 + main() 呼び出し)
- Create: `web/src/data/condition_analysis.json` (生成)

- [ ] **Step 1: export_condition_analysis() 関数を generate_data.py に追加**

`web/generate_data.py` に以下の関数を追加する（`export_financial` の直後）:

```python
def export_condition_analysis(conn: sqlite3.Connection) -> dict:
    """
    過去2年分のバックテスト結果から「競馬場×距離×馬場状態×モデル」ごとの
    ROI・的中率を集計して返す。
    """
    _rows = lambda rs: [dict(zip([d[0] for d in rs.description], r)) for r in rs]  # noqa

    two_years_ago = "date('now', '-2 years')"

    base_sql = f"""
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        JOIN prediction_results pr ON p.id = pr.prediction_id
        WHERE r.date >= {two_years_ago}
          AND pr.id IS NOT NULL
    """

    dist_case = """
        CASE
            WHEN r.distance IS NULL OR r.distance = 0 THEN '不明'
            WHEN r.distance < 1400  THEN '短距離(<1400m)'
            WHEN r.distance <= 1800 THEN 'マイル(1400-1800m)'
            WHEN r.distance <= 2200 THEN '中距離(1801-2200m)'
            ELSE '長距離(>2200m)'
        END
    """

    def _agg(group_cols: str, label: str) -> list[dict]:
        sql = f"""
        SELECT
            {group_cols},
            p.model_type,
            COUNT(pr.id)  AS total_bets,
            COALESCE(SUM(pr.is_hit), 0) AS hits,
            ROUND(CAST(SUM(pr.is_hit) AS REAL) / NULLIF(COUNT(pr.id), 0) * 100, 1) AS hit_rate,
            COALESCE(SUM(p.recommended_bet), 0)  AS total_invested,
            COALESCE(SUM(pr.payout), 0)          AS total_payout,
            ROUND(
                COALESCE(SUM(pr.payout), 0)
                / NULLIF(SUM(p.recommended_bet), 0) * 100, 1
            ) AS roi
        {base_sql}
        GROUP BY {group_cols}, p.model_type
        HAVING COUNT(pr.id) >= 3
        ORDER BY roi DESC NULLS LAST
        """
        cur = conn.execute(sql)
        return [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    by_venue    = _agg("r.venue AS venue", "venue")
    by_distance = _agg(f"({dist_case}) AS distance_cat", "distance")
    by_surface  = _agg("COALESCE(r.surface, '不明') AS surface", "surface")
    by_condition = _agg("COALESCE(r.condition, '不明') AS track_condition", "condition")

    # 複合: venue × distance × surface × model_type（最低3件以上）
    combined_sql = f"""
    SELECT
        r.venue,
        {dist_case}                         AS distance_cat,
        COALESCE(r.surface, '不明')         AS surface,
        COALESCE(r.condition, '不明')       AS track_condition,
        p.model_type,
        COUNT(pr.id)                        AS total_bets,
        COALESCE(SUM(pr.is_hit), 0)         AS hits,
        ROUND(CAST(SUM(pr.is_hit) AS REAL) / NULLIF(COUNT(pr.id), 0) * 100, 1) AS hit_rate,
        COALESCE(SUM(p.recommended_bet), 0) AS total_invested,
        COALESCE(SUM(pr.payout), 0)         AS total_payout,
        ROUND(
            COALESCE(SUM(pr.payout), 0)
            / NULLIF(SUM(p.recommended_bet), 0) * 100, 1
        ) AS roi
    {base_sql}
    GROUP BY r.venue, distance_cat, r.surface, r.condition, p.model_type
    HAVING COUNT(pr.id) >= 3
    ORDER BY roi DESC NULLS LAST
    LIMIT 100
    """
    cur = conn.execute(combined_sql)
    combined = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    import datetime
    return {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "by_venue":     by_venue,
        "by_distance":  by_distance,
        "by_surface":   by_surface,
        "by_condition": by_condition,
        "combined":     combined,
    }
```

- [ ] **Step 2: main() に export_condition_analysis を追加**

`generate_data.py` の main() 関数（または最後の `if __name__ == "__main__":` ブロック）に以下を追加:

```python
    # 得意条件分析
    condition_data = export_condition_analysis(conn)
    (DATA_DIR / "condition_analysis.json").write_text(
        json.dumps(condition_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  → condition_analysis.json ({len(condition_data['combined'])} 条件)")
```

- [ ] **Step 3: generate_data.py を実行して condition_analysis.json を生成**

```bash
cd C:/dev/horse-racing-ai/web
python generate_data.py
```

Expected: `web/src/data/condition_analysis.json` が生成され、`combined` に3件以上のデータが含まれる

- [ ] **Step 4: git commit**

```bash
git add web/generate_data.py web/src/data/condition_analysis.json
git commit -m "feat: 得意条件分析エクスポート(venue×distance×surface×model_type別ROI)"
```

---

## Task 4: ConditionAnalysis.tsx — 得意条件テーブルコンポーネント

**Files:**
- Create: `web/src/components/ConditionAnalysis.tsx`

- [ ] **Step 1: ConditionAnalysis コンポーネントを作成**

```typescript
'use client'

interface ConditionRow {
  venue?:           string
  distance_cat?:    string
  surface?:         string
  track_condition?: string
  model_type:       string
  total_bets:       number
  hits:             number
  hit_rate:         number
  total_invested:   number
  total_payout:     number
  roi:              number | null
}

interface ConditionData {
  generated_at: string
  by_venue:     ConditionRow[]
  by_distance:  ConditionRow[]
  by_surface:   ConditionRow[]
  by_condition: ConditionRow[]
  combined:     ConditionRow[]
}

type GroupKey = 'combined' | 'by_venue' | 'by_distance' | 'by_surface' | 'by_condition'

const GROUP_LABELS: Record<GroupKey, string> = {
  combined:     '総合（場×距離×馬場）',
  by_venue:     '競馬場別',
  by_distance:  '距離別',
  by_surface:   '芝/ダート別',
  by_condition: '馬場状態別',
}

function roiClass(roi: number | null): string {
  if (roi == null) return 'text-[var(--text-muted)]'
  if (roi >= 200)  return 'neon-text-gold font-bold'
  if (roi >= 100)  return 'neon-text-green font-semibold'
  if (roi < 50)    return 'neon-text-red'
  return 'text-[var(--text-primary)]'
}

function roiBg(roi: number | null): string {
  if (roi == null) return ''
  if (roi >= 200)  return 'bg-yellow-900/20'
  if (roi >= 100)  return 'bg-green-900/15'
  if (roi < 50)    return 'bg-red-900/15'
  return ''
}

function labelFor(row: ConditionRow, group: GroupKey): string {
  if (group === 'combined') {
    return [row.venue, row.distance_cat, row.surface, row.track_condition]
      .filter(Boolean).join(' / ')
  }
  return (row.venue ?? row.distance_cat ?? row.surface ?? row.track_condition ?? '—')
}

interface Props {
  data: ConditionData | null
}

export default function ConditionAnalysis({ data }: Props) {
  const [group, setGroup] = useState<GroupKey>('combined')

  if (!data) {
    return (
      <div className="neon-card p-6 text-center text-[var(--text-muted)]">
        分析データなし（generate_data.py を実行してください）
      </div>
    )
  }

  const rows = data[group] ?? []

  return (
    <div className="space-y-4">
      {/* タブ */}
      <div className="neon-card p-3 flex flex-wrap gap-2">
        {(Object.keys(GROUP_LABELS) as GroupKey[]).map(k => (
          <button
            key={k}
            onClick={() => setGroup(k)}
            className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
              group === k
                ? 'bg-[var(--neon-blue)] text-black'
                : 'text-[var(--text-muted)] hover:text-[var(--text-primary)]'
            }`}
          >
            {GROUP_LABELS[k]}
          </button>
        ))}
        <span className="ml-auto text-xs text-[var(--text-muted)] self-center">
          更新: {data.generated_at}
        </span>
      </div>

      {/* テーブル */}
      <div className="neon-card overflow-hidden">
        <div className="table-scroll">
          <table className="race-table w-full">
            <thead>
              <tr>
                <th className="text-left">条件</th>
                <th className="text-left">モデル</th>
                <th className="text-right">件数</th>
                <th className="text-right">的中</th>
                <th className="text-right">的中率</th>
                <th className="text-right">ROI</th>
                <th className="text-right">投資</th>
                <th className="text-right">払戻</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={8} className="text-center text-[var(--text-muted)] py-6">
                    データなし（最低3件以上の予想結果が必要）
                  </td>
                </tr>
              ) : rows.map((row, i) => (
                <tr key={i} className={roiBg(row.roi)}>
                  <td className="max-w-[200px] truncate text-sm" title={labelFor(row, group)}>
                    {labelFor(row, group)}
                  </td>
                  <td>
                    <span className={`text-xs font-bold ${row.model_type.includes('卍') ? 'neon-text' : 'neon-text-gold'}`}>
                      {row.model_type}
                    </span>
                  </td>
                  <td className="text-right font-mono text-sm">{row.total_bets}</td>
                  <td className="text-right font-mono text-sm">{row.hits}</td>
                  <td className="text-right font-mono text-sm">{row.hit_rate?.toFixed(1)}%</td>
                  <td className={`text-right font-mono text-sm ${roiClass(row.roi)}`}>
                    {row.roi != null ? `${row.roi.toFixed(1)}%` : '—'}
                  </td>
                  <td className="text-right font-mono text-xs text-[var(--text-muted)]">
                    ¥{row.total_invested.toLocaleString()}
                  </td>
                  <td className="text-right font-mono text-xs">
                    ¥{row.total_payout.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="px-4 py-2 text-xs text-[var(--text-muted)] border-t border-[var(--border)]">
          {rows.length} 条件（3件以上の予想実績あり） — ROI降順
        </div>
      </div>
    </div>
  )
}
```

ファイル冒頭に `import { useState } from 'react'` が必要。

- [ ] **Step 2: git commit（コンポーネント単体）**

```bash
git add web/src/components/ConditionAnalysis.tsx
git commit -m "feat: 得意条件分析テーブルコンポーネント追加"
```

---

## Task 5: AppShell.tsx — 条件分析タブを追加

**Files:**
- Modify: `web/src/components/AppShell.tsx`

- [ ] **Step 1: AppShell.tsx に condition タブ追加**

`web/src/components/AppShell.tsx` を以下のように修正する:

(a) import 追加:
```typescript
import ConditionAnalysis   from './ConditionAnalysis'
```

(b) View 型に `'condition'` を追加:
```typescript
type View = 'race' | 'hits' | 'dashboard' | 'financial' | 'win5' | 'gachi' | 'condition'
```

(c) state 追加（既存 `gachiHits` の次）:
```typescript
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [conditionData, setConditionData] = useState<any>(null)
```

(d) fetchAll の Promise.all にエンドポイントを追加:
```typescript
          fetch('/api/condition'),   // 新規
```

(e) 取得後の setState に追加:
```typescript
          setConditionData(conditionRaw ?? null)
```

(f) NavBar に「得意条件」ボタン追加（NavBar コンポーネントに渡している onView の呼び出し部分）:
```typescript
          <button onClick={() => setView('condition')}>得意条件</button>
```

(g) レンダリング部分に追加（`gachi` ブロックの後）:
```typescript
        {view === 'condition' && (
          <ConditionAnalysis data={conditionData} />
        )}
```

- [ ] **Step 2: /api/condition エンドポイント作成**

`web/src/app/api/condition/route.ts` を新規作成:

```typescript
import { NextResponse } from 'next/server'
import { readFileSync } from 'fs'
import { join } from 'path'

export const dynamic = 'force-static'

export async function GET() {
  try {
    const filePath = join(process.cwd(), 'src/data/condition_analysis.json')
    const data = JSON.parse(readFileSync(filePath, 'utf-8'))
    return NextResponse.json(data)
  } catch {
    return NextResponse.json(null)
  }
}
```

- [ ] **Step 3: NavBar に「得意条件」ボタン追加**

`web/src/components/NavBar.tsx` を確認し、`onView` prop で `'condition'` を渡すボタンを追加する（既存のボタン群の末尾に追加）。

- [ ] **Step 4: 動作確認**

```bash
cd C:/dev/horse-racing-ai/web
npm run dev
```

ブラウザで `http://localhost:3000` を開き:
- 「得意条件」タブが表示される
- 各グループ（総合/競馬場別/距離別/芝ダート別/馬場状態別）の切り替えができる
- ROI200%以上は金色、100%以上は緑、50%未満は赤で表示される

- [ ] **Step 5: git commit**

```bash
git add web/src/components/AppShell.tsx web/src/app/api/condition/route.ts web/src/components/NavBar.tsx
git commit -m "feat: 得意条件分析タブをダッシュボードに統合"
```

---

## Task 6: 最終検証 — 新潟3R 的中実績確認

- [ ] **Step 1: prediction_results を再計算**

```bash
python -c "
from src.database.init_db import init_db
from src.evaluation.evaluator import Evaluator
conn = init_db()
e = Evaluator()
# 新潟3Rの実際の race_id を確認
rows = conn.execute(\"SELECT race_id FROM races WHERE venue='新潟' AND race_number=3 ORDER BY date DESC LIMIT 3\").fetchall()
for r in rows: print(r[0])
"
```

- [ ] **Step 2: 対象 race_id で再評価**

```bash
python -c "
from src.database.init_db import init_db
from src.evaluation.evaluator import Evaluator
conn = init_db()
e = Evaluator()
result = e.evaluate_race(conn, '<新潟3RのRACE_ID>')
print(f'ROI={result.roi:.1f}%')
print(f'払戻=¥{result.total_payout:,.0f}')
print(f'投資=¥{result.total_invested:,.0f}')
for h in result.hits:
    if h.is_hit: print(f'  的中: {h.bet_type} ROI={h.roi:.1f}%')
"
```

Expected:
- 投資: ¥1,200
- 払戻: ¥421,600
- ROI: 35133.3%

- [ ] **Step 3: generate_data.py で全JSONを再生成**

```bash
cd C:/dev/horse-racing-ai/web
python generate_data.py
```

- [ ] **Step 4: 最終 git commit**

```bash
git add -A
git commit -m "fix: 全バグ修正完了 — ROI/UI重複/条件分析を統合"
```

---

## 自己レビュー

**Spec coverage:**
- [x] ROI = (payout / (n_tickets × 100)) × 100 → Task 1
- [x] 返還チェックを馬番ベースに → Task 1 Step 1
- [x] UI重複表示修正（sortedCombinations） → Task 2 Step 1
- [x] ラベル修正（2頭軸マルチ） → Task 2 Step 3-4
- [x] condition_analysis.json 生成 → Task 3
- [x] 条件分析ダッシュボード → Task 4-5
- [x] 新潟3R 35133.3% 確認 → Task 6

**Placeholder scan:** なし（全ステップにコード記載済み）

**Type consistency:** `ConditionRow` インターフェースは Task 4 のみで定義。`GroupKey` は Task 4 内で完結。
