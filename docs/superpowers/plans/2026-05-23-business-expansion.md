# Business Expansion — UI 4タブ化 / Discord完全リアル化 / ドキュメント永続化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** RaceDetailを4サブタブ化・Discord通知を完全リアル化・明日の予想をUIに反映・運用ドキュメントを永続化する

**Architecture:** UI変更はRaceDetail.tsx1ファイルのみ（新サブコンポーネントをファイル末尾に追加）。Discord変更はdiscord_notifier.pyの既存クラスに最小修正。予想UIはgenerate_data.pyで再生成。ドキュメントは新規mdファイル2本。

**Tech Stack:** TypeScript/React (Next.js 15), Python 3.11, SQLite, Discord Webhook API

---

## 診断済み事実（実装前確認不要）

- DB: `predictions` に 2026-05-24 分が **288件**（model=`本命(暫定)`）存在 ✅
- UI: `web/src/data/predictions.json` の 2026-05-24 エントリは **0件** ❌ → `generate_data.py` 未実行が原因
- 修正コマンド: `python web/generate_data.py`

---

## ファイル変更一覧

| ファイル | 種別 | タスク |
|---------|------|--------|
| `web/src/components/RaceDetail.tsx` | 変更 | Task 2 |
| `src/notification/discord_notifier.py` | 変更 | Task 3 |
| `web/generate_data.py` | 実行のみ | Task 1 |
| `docs/automation_schedule.md` | 新規 | Task 4 |
| `docs/roadmap.md` | 新規 | Task 4 |

---

## Task 1: 緊急 — generate_data.py 実行で明日の予想をUIに反映

**Files:**
- Run: `web/generate_data.py`
- Update: `web/src/data/predictions.json`

- [ ] **Step 1: UIの現状確認**

```bash
py -c "import json; d=json.load(open('web/src/data/predictions.json', encoding='utf-8')); t=[p for p in d if p['date']=='2026-05-24']; print(f'2026-05-24: {len(t)}件')"
```
Expected: `2026-05-24: 0件`

- [ ] **Step 2: generate_data.py を実行**

```bash
python web/generate_data.py
```
Expected: 成功ログ（エラーなし）、`predictions.json` / `races/*.json` が更新される

- [ ] **Step 3: 更新確認**

```bash
py -c "import json; d=json.load(open('web/src/data/predictions.json', encoding='utf-8')); t=[p for p in d if p['date']=='2026-05-24']; print(f'2026-05-24: {len(t)}件'); print('sample:', t[0]['model_type'] if t else 'NONE')" 2>&1
```
Expected: `2026-05-24: 288件（以上）` / model_type = `本命(暫定)`

- [ ] **Step 4: Commit**

```bash
git add web/src/data/predictions.json web/src/data/races.json web/src/data/financial.json web/src/data/gachi_hits.json web/src/data/condition_analysis.json
git add web/src/data/races/
git commit -m "data: generate_data.py — 2026-05-24 暫定予想 288件を UI JSON に反映"
```

---

## Task 2: UI — RaceDetail 4サブタブ化 + SNSコピーボタン

**Files:**
- Modify: `web/src/components/RaceDetail.tsx`

### Step 2-1: Tab型定義と state・tabs配列を書き換える

- [ ] **Step 2-1: Tab型・デフォルト・tabs配列を変更**

`web/src/components/RaceDetail.tsx` の以下箇所を変更:

**変更前 (line 8):**
```typescript
type Tab = 'results' | 'prerace' | 'predictions'
```

**変更後:**
```typescript
type Tab = 'race_card' | 'results' | 'predictions' | 'payouts'
```

**変更前 (lines 57-59):**
```typescript
  const hasPrerace = !!race.prerace
  const defaultTab: Tab = hasPrerace ? 'prerace' : 'results'
  const [tab, setTab] = useState<Tab>(defaultTab)
```

**変更後:**
```typescript
  const hasPrerace = !!race.prerace
  const [tab, setTab] = useState<Tab>('race_card')
```

**変更前 (lines 74-78):**
```typescript
  const tabs: { key: Tab; label: string; count?: number }[] = [
    ...(hasPrerace ? [{ key: 'prerace' as Tab, label: 'AI直前分析' }] : []),
    { key: 'results',     label: 'レース結果' },
    { key: 'predictions', label: 'AI予想', count: hasPredictions ? predictions.length : undefined },
  ]
```

**変更後:**
```typescript
  const tabs: { key: Tab; label: string; count?: number }[] = [
    { key: 'race_card',   label: '出馬表' },
    { key: 'results',     label: 'レース結果' },
    { key: 'predictions', label: 'AI予想', count: hasPredictions ? predictions.length : undefined },
    { key: 'payouts',     label: '的中結果', count: hasPayouts ? betTypes.length : undefined },
  ]
```

- [ ] **Step 2-2: タブコンテンツ描画部分を4タブに全面差し替え**

`web/src/components/RaceDetail.tsx` の以下の範囲を差し替える:

**変更前 (lines 163-214 — タブコンテンツ3ブロック):**
```typescript
      {/* ── AI直前分析タブ ─────────────────────────── */}
      {tab === 'prerace' && hasPrerace && (
        <div className="space-y-4">
          {/* バイアスパネル */}
          <BiasPanel
            bias={race.prerace!.bias}
            condition={race.condition || ''}
          />

          {/* AI直前予想テーブル */}
          <PreraceTable results={race.results ?? []} />
        </div>
      )}

      {/* ── レース結果タブ ─────────────────────────── */}
      {tab === 'results' && (
        <div className="space-y-4">
          <ResultsTable results={race.results ?? []} />
          {hasPayouts && (
            <div className="neon-card overflow-hidden">
              <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
                <span className="text-sm neon-text tracking-[0.2em] font-semibold">
                  PAYOUTS — 払戻金
                </span>
              </div>
              <div className="p-4 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                {betTypes.map(betType => (
                  <PayoutCard
                    key={betType}
                    betType={betType}
                    payouts={payoutsByType[betType]}
                  />
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── AI予想タブ ────────────────────────────── */}
      {tab === 'predictions' && (
        hasPredictions
          ? <PredictionsPanel predictions={predictions} limit={200} payouts={race.payouts ?? []} />
          : (
            <div className="neon-card p-12 text-center">
              <div className="text-[var(--text-muted)] text-base tracking-widest">
                このレースの予想データはありません
              </div>
            </div>
          )
      )}
    </div>
  )
}
```

**変更後:**
```typescript
      {/* ── 出馬表タブ ────────────────────────────── */}
      {tab === 'race_card' && (
        <RaceCardTable results={race.results ?? []} />
      )}

      {/* ── レース結果タブ ─────────────────────────── */}
      {tab === 'results' && (
        <ResultsTable results={race.results ?? []} />
      )}

      {/* ── AI予想タブ ────────────────────────────── */}
      {tab === 'predictions' && (
        <div className="space-y-4">
          <SnsShareButton race={race} predictions={predictions} />
          {hasPrerace && (
            <>
              <BiasPanel
                bias={race.prerace!.bias}
                condition={race.condition || ''}
              />
              <PreraceTable results={race.results ?? []} />
            </>
          )}
          {hasPredictions
            ? <PredictionsPanel predictions={predictions} limit={200} payouts={race.payouts ?? []} />
            : (
              <div className="neon-card p-12 text-center">
                <div className="text-[var(--text-muted)] text-base tracking-widest">
                  このレースの予想データはありません
                </div>
              </div>
            )
          }
        </div>
      )}

      {/* ── 的中結果タブ ──────────────────────────── */}
      {tab === 'payouts' && (
        hasPayouts
          ? (
            <div className="neon-card overflow-hidden">
              <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
                <span className="text-sm neon-text tracking-[0.2em] font-semibold">
                  PAYOUTS — 払戻金
                </span>
              </div>
              <div className="p-4 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                {betTypes.map(betType => (
                  <PayoutCard
                    key={betType}
                    betType={betType}
                    payouts={payoutsByType[betType]}
                  />
                ))}
              </div>
            </div>
          )
          : (
            <div className="neon-card p-12 text-center">
              <div className="text-[var(--text-muted)] text-base tracking-widest">
                払戻データはありません
              </div>
            </div>
          )
      )}
    </div>
  )
}
```

- [ ] **Step 2-3: RaceCardTable サブコンポーネントを追加**

`web/src/components/RaceDetail.tsx` の `// ── AI直前分析テーブル` のコメント行の直前（`function PreraceTable` より前）に追加:

```typescript
// ── 出馬表テーブル ────────────────────────────────────────
function RaceCardTable({ results }: { results: RaceResult[] }) {
  const sorted = [...results].sort((a, b) => (a.horse_number ?? 99) - (b.horse_number ?? 99))

  return (
    <div className="neon-card overflow-hidden">
      <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">
          RACE CARD — 出馬表
        </span>
      </div>

      {/* デスクトップ */}
      <div className="hidden md:block table-scroll">
        <table className="w-full race-table">
          <thead>
            <tr>
              <th className="text-center">枠</th>
              <th className="text-center">馬番</th>
              <th className="text-left">馬名</th>
              <th>性齢</th>
              <th className="text-right">斤量</th>
              <th>騎手</th>
              <th>厩舎</th>
              <th className="text-right">単勝</th>
              <th className="text-center">人気</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={r.horse_name + i}>
                <td className="text-center">
                  {r.gate_number != null ? <GateBadge gate={r.gate_number} /> : <span className="text-[var(--text-muted)]">—</span>}
                </td>
                <td className="text-center font-mono text-[var(--text-muted)]">{r.horse_number ?? '—'}</td>
                <td>
                  <span className="font-semibold text-[var(--text-primary)]">{r.horse_name}</span>
                </td>
                <td className="text-[var(--text-muted)]">{r.sex_age}</td>
                <td className="text-right font-mono">{r.weight_carried}</td>
                <td>{r.jockey}</td>
                <td className="text-[var(--text-muted)]">{r.trainer || '—'}</td>
                <td className="text-right font-mono"><OddsCell odds={r.win_odds} /></td>
                <td className="text-center font-mono text-[var(--text-muted)]">
                  {r.popularity != null ? `${r.popularity}人気` : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* モバイル */}
      <div className="md:hidden">
        <div style={{ padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: 6 }}>
          {sorted.map((r, i) => (
            <div key={r.horse_name + i} className="horse-row-card">
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3, paddingTop: 2 }}>
                <span className="horse-num-lg">{r.horse_number}</span>
                {r.gate_number != null && <GateBadge gate={r.gate_number} />}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <span style={{ fontSize: '0.92rem', fontWeight: 700, color: 'var(--text-primary)' }}>
                  {r.horse_name}
                </span>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>
                  {r.sex_age} · {r.weight_carried}kg · {r.jockey}
                  {r.trainer ? ` / ${r.trainer}` : ''}
                </div>
                <div style={{ display: 'flex', gap: 12 }}>
                  <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                    単勝 <OddsCell odds={r.win_odds} />
                  </span>
                  {r.popularity != null && (
                    <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                      {r.popularity}人気
                    </span>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
```

- [ ] **Step 2-4: SnsShareButton サブコンポーネントを追加**

`web/src/components/RaceDetail.tsx` の `// ── 出馬表テーブル` の直前に追加。
インポートの先頭に `'use client'` はファイル冒頭にあるので追加不要。

```typescript
// ── SNS投稿用テキスト生成・コピーボタン ──────────────────────
function buildSnsText(
  race: RaceEntry & { prerace?: { ev_recommend: EvRecommend[]; bias: RaceBias; generated_at: string } },
  predictions: Prediction[],
): string {
  const evRecs = race.prerace?.ev_recommend ?? []
  const topBets = predictions
    .filter(p => (p.expected_value ?? 0) >= 1.0)
    .slice(0, 6)

  const header = `📊 UMALOGI AI予想 | ${race.date} ${race.venue}${race.race_number}R — ${race.race_name || ''}`

  const evSection = evRecs.length > 0
    ? `\n🔥【激アツ推奨馬 — EV≥1.0】\n` +
      evRecs.slice(0, 5).map(h =>
        `  ⬛ ${h.horse_number}番 ${h.horse_name}  EV=${h.ev_score.toFixed(2)}`
      ).join('\n')
    : ''

  const betSection = topBets.length > 0
    ? `\n\n【AI買い目（EV≥1.0）】\n` +
      topBets.map(p => {
        const nStr = p.n_tickets > 0 ? ` ${p.n_tickets}点` : ''
        return `  ${p.model_type}: ${p.bet_type}${nStr}`
      }).join('\n')
    : ''

  const footer = [
    '',
    '─────────────────',
    '🆓 1レース目は無料公開中',
    '📲 フォロー＆リポストで最新AI予想をチェック',
    '#競馬予想 #AI競馬 #UMALOGI',
  ].join('\n')

  return [header, evSection, betSection, footer].join('')
}

function SnsShareButton({
  race,
  predictions,
}: {
  race: RaceEntry & { prerace?: { ev_recommend: EvRecommend[]; bias: RaceBias; generated_at: string } }
  predictions: Prediction[]
}) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    const text = buildSnsText(race, predictions)
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // clipboard API not available (non-HTTPS)
    }
  }

  return (
    <div className="flex justify-end">
      <button
        onClick={handleCopy}
        className={`px-4 py-2 text-sm font-semibold rounded-md tracking-wider transition-colors ${
          copied
            ? 'bg-[rgba(0,200,100,0.2)] text-[var(--neon-green)] border border-[rgba(0,200,100,0.4)]'
            : 'bg-[rgba(0,200,255,0.1)] text-[var(--neon-cyan)] border border-[rgba(0,200,255,0.3)] hover:bg-[rgba(0,200,255,0.2)]'
        }`}
      >
        {copied ? '✅ コピーしました!' : '📋 SNS投稿テキストをコピー'}
      </button>
    </div>
  )
}
```

- [ ] **Step 2-5: Prediction 型インポート確認 + TypeScriptビルド確認**

`web/src/components/RaceDetail.tsx` 先頭のインポートに `Prediction` が含まれているか確認:
```typescript
import type { RaceEntry, Prediction, RacePayout, RaceResult, TrainingEval, RaceBias, EvRecommend } from '@/types/race'
```
既にある場合は変更不要。

ビルド確認:
```bash
cd web && npx tsc --noEmit 2>&1
```
Expected: エラーなし（または警告のみ）

- [ ] **Step 2-6: Commit**

```bash
git add web/src/components/RaceDetail.tsx
git commit -m "feat: RaceDetail を出馬表/レース結果/AI予想/的中結果の4サブタブに再編成 + SNS投稿コピーボタン追加"
```

---

## Task 3: Discord — 的中速報チャンネル分離 + 通知完全リアル化

**Files:**
- Modify: `src/notification/discord_notifier.py`

### 変更1: DISCORD_WEBHOOK_HIT_FLASH 対応

- [ ] **Step 3-1: __init__ に hit_flash_url を追加**

`src/notification/discord_notifier.py` の `__init__` を変更:

**変更前 (lines 77-92):**
```python
    def __init__(
        self,
        *,
        webhook_url:    str | None = None,
        system_url:     str | None = None,
        enabled: bool = True,
        channel_label:  str = "予想",
    ) -> None:
        super().__init__(enabled=enabled)
        self._url        = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL", "")
        self._system_url = system_url  or os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL", "")
        self._label      = channel_label
        if enabled and not self._url:
            logger.warning("DISCORD_WEBHOOK_URL が設定されていません（予想通知が届きません）")
        if enabled and not self._system_url:
            logger.warning("DISCORD_SYSTEM_WEBHOOK_URL が設定されていません（システム通知は予想チャンネルへ fallback します）")
```

**変更後:**
```python
    def __init__(
        self,
        *,
        webhook_url:    str | None = None,
        system_url:     str | None = None,
        hit_flash_url:  str | None = None,
        enabled: bool = True,
        channel_label:  str = "予想",
    ) -> None:
        super().__init__(enabled=enabled)
        self._url           = webhook_url   or os.environ.get("DISCORD_WEBHOOK_URL", "")
        self._system_url    = system_url    or os.environ.get("DISCORD_SYSTEM_WEBHOOK_URL", "")
        self._hit_flash_url = hit_flash_url or os.environ.get("DISCORD_WEBHOOK_HIT_FLASH", "")
        self._label         = channel_label
        if enabled and not self._url:
            logger.warning("DISCORD_WEBHOOK_URL が設定されていません（予想通知が届きません）")
        if enabled and not self._system_url:
            logger.warning("DISCORD_SYSTEM_WEBHOOK_URL が設定されていません（システム通知は予想チャンネルへ fallback します）")
```

### 変更2: notify_hit_summary を的中速報専用チャンネルへ

- [ ] **Step 3-2: notify_hit_summary の送信先を変更**

`notify_hit_summary` メソッド内の以下を変更:

**変更前 (line 308):**
```python
        ok = self._post(self._url, payload)
        logger.info("[Discord:予想] 結果サマリー %s: %s", "送信完了" if ok else "失敗", date_str)
```

**変更後:**
```python
        hit_url = self._hit_flash_url or self._url
        ok = self._post(hit_url, payload)
        ch_label = "的中速報" if self._hit_flash_url else "予想(fallback)"
        logger.info("[Discord:%s] 結果サマリー %s: %s", ch_label, "送信完了" if ok else "失敗", date_str)
```

### 変更3: 直前予想通知に購入単価×点数を明記

- [ ] **Step 3-3: notify_prerace_result の field_name に購入コストを追加**

`notify_prerace_result` メソッド内、`for bet in bets[:max_bets]:` ブロックの以下を変更:

**変更前 (lines 381-387):**
```python
                ev        = getattr(bet, "expected_value", 0.0)
                bet_type  = getattr(bet, "bet_type", "?")
                rec_bet   = int(getattr(bet, "recommended_bet", 0) or 0)
                fire      = _FIRE if ev >= 1.0 else "　"

                # フィールド name: "🔥 三連複  EV=2.13  ¥800"
                field_name = f"{fire} {bet_type}  EV={ev:.2f}  ¥{rec_bet:,}"
```

**変更後:**
```python
                ev        = getattr(bet, "expected_value", 0.0)
                bet_type  = getattr(bet, "bet_type", "?")
                rec_bet   = int(getattr(bet, "recommended_bet", 0) or 0)
                fire      = _FIRE if ev >= 1.0 else "　"
                combos    = getattr(bet, "combinations", []) or []
                n_combos  = len(combos)
                cost_str  = f"¥100×{n_combos}点=¥{n_combos * 100:,}" if n_combos > 0 else f"¥{rec_bet:,}"

                # フィールド name: "🔥 三連複  EV=2.13 | ¥100×4点=¥400"
                field_name = f"{fire} {bet_type}  EV={ev:.2f} | {cost_str}"
```

### 変更4: _format_combo_card — 馬番全表示・省略撤廃・軸スマート表記

- [ ] **Step 3-4: _format_combo_card の全省略制限を撤廃**

`_format_combo_card` 関数全体を以下に差し替える（Discord 1024文字バジェット制御付き）:

```python
def _format_combo_card(bet: object) -> str:
    """
    買い目をスマホ対応カード形式にフォーマット。馬番を省略せず全表示。

    出力例:
      複勝:
        ⬛ 5番 アーバンシック
        ⬛ 9番 キタノオウジ

      三連複 (軸1頭流し):
        【推奨: 三連複流し 軸5 - 相手3,7,9,12】
        ▶ 軸: 5番 アーバンシック
          相手: 3番 / 7番 / 9番 / 12番
          計4点

      三連単:
        ▶ 5→9→3
        ▶ 5→3→9
        ▶ 5→7→3
        ▶ 5→7→9
    """
    from collections import Counter

    _BUDGET = 900  # Discord field.value 上限 1024 に余裕を持たせたバジェット

    bt: str      = getattr(bet, "bet_type", "")
    combos: list = getattr(bet, "combinations", []) or []
    names: list  = getattr(bet, "horse_names",  []) or []

    if not combos:
        return "　(買い目なし)"

    n_total = len(combos)

    # horse_number → 馬名 逆引きマップ
    name_by_num: dict[int, str] = {}

    if bt in ("単勝", "複勝"):
        for i, combo in enumerate(combos):
            num = combo[0] if isinstance(combo, (list, tuple)) else combo
            if i < len(names) and names[i]:
                name_by_num[int(num)] = str(names[i])
    else:
        first = combos[0]
        first_legs = list(first) if isinstance(first, (list, tuple)) else [first]
        for i, leg in enumerate(first_legs):
            if i < len(names) and names[i]:
                name_by_num[int(leg)] = str(names[i])

    def _label(num: int) -> str:
        n = name_by_num.get(int(num), "")
        return f"{num}番 {n}" if n else f"{num}番"

    def _fit(lines: list[str], budget: int = _BUDGET) -> str:
        """行リストを budget 文字以内に収める。超える場合は末尾に省略行を追記。"""
        out: list[str] = []
        used = 0
        for line in lines:
            if used + len(line) + 1 > budget:
                remaining = n_total - len(out)
                if remaining > 0:
                    out.append(f"  …(残り{remaining}点 省略)")
                break
            out.append(line)
            used += len(line) + 1
        return "\n".join(out)

    # ── 単勝・複勝 ──────────────────────────────────────────────────────────
    if bt in ("単勝", "複勝"):
        nums = [c[0] if isinstance(c, (list, tuple)) else c for c in combos]
        lines = [f"⬛ {_label(n)}" for n in nums]
        return _fit(lines)

    # ── 馬単・三連単（順序あり）────────────────────────────────────────────
    if bt in ("馬単", "三連単"):
        lines = []
        for combo in combos:
            legs = list(combo) if isinstance(combo, (list, tuple)) else [combo]
            # 番号のみで表示（Discord文字数節約）
            arrow_str = "→".join(str(n) for n in legs)
            lines.append(f"▶ {arrow_str}")
        return _fit(lines)

    # ── 馬連・ワイド・三連複（軸流し or ボックス）──────────────────────────
    first = combos[0]
    if isinstance(first, (list, tuple)) and len(first) >= 2:
        flat     = [int(n) for combo in combos for n in combo]
        cnt      = Counter(flat)
        axis_set = {num for num, c in cnt.items() if c == n_total}

        if axis_set:
            # 軸あり: 全相手馬を表示
            axes   = sorted(axis_set)
            others = sorted({int(n) for combo in combos for n in combo} - axis_set)
            axis_str  = ",".join(str(a) for a in axes)
            opp_nums  = ",".join(str(o) for o in others)
            axis_label = " / ".join(_label(a) for a in axes[:2])
            opp_label  = " / ".join(_label(o) for o in others)
            smart_str  = f"【推奨: {bt}流し 軸{axis_str} - 相手{opp_nums}】"
            detail     = (
                f"▶ 軸: {axis_label}\n"
                f"  相手: {opp_label}\n"
                f"  計{n_total}点"
            )
            result = smart_str + "\n" + detail
            if len(result) <= _BUDGET:
                return result
            # 超える場合は番号のみ
            return f"{smart_str}\n計{n_total}点"
        else:
            # ボックス: 全馬番を表示
            nums_all = sorted({int(n) for combo in combos for n in combo})
            all_labels = " / ".join(_label(n) for n in nums_all)
            box_str = f"ボックス: {all_labels}\n計{n_total}点"
            if len(box_str) <= _BUDGET:
                return box_str
            # 超える場合は番号のみ
            nums_str = ",".join(str(n) for n in nums_all)
            return f"ボックス: {nums_str}\n計{n_total}点"

    return f"計{n_total}点"
```

- [ ] **Step 3-5: ドライラン確認（オプション）**

```bash
python -c "
import sys; sys.path.insert(0, '.')
from src.notification.discord_notifier import _format_combo_card

class MockBet:
    def __init__(self, bt, combos, names=[]):
        self.bet_type = bt
        self.combinations = combos
        self.horse_names = names

# 三連複軸流しテスト
bet = MockBet('三連複', [[5,3,7],[5,3,9],[5,7,9],[5,3,12]], ['アーバン','レガシー','サクセス'])
print('=== 三連複軸流し ===')
print(_format_combo_card(bet))

# 三連単全表示テスト
bet2 = MockBet('三連単', [[5,9,3],[5,3,9],[5,7,3],[5,7,9]])
print('\n=== 三連単 ===')
print(_format_combo_card(bet2))
" 2>&1
```

Expected:
```
=== 三連複軸流し ===
【推奨: 三連複流し 軸5 - 相手3,7,9,12】
▶ 軸: 5番 アーバン
  相手: 3番 レガシー / 7番 サクセス / 9番 / 12番
  計4点

=== 三連単 ===
▶ 5→9→3
▶ 5→3→9
▶ 5→7→3
▶ 5→7→9
```

- [ ] **Step 3-6: Commit**

```bash
git add src/notification/discord_notifier.py
git commit -m "feat: Discord通知刷新 — 的中速報チャンネル分離(HIT_FLASH)/購入単価明記/馬番全表示/軸推奨スマート表記"
```

---

## Task 4: docs — automation_schedule.md と roadmap.md 新規作成

**Files:**
- Create: `docs/automation_schedule.md`
- Create: `docs/roadmap.md`

- [ ] **Step 4-1: docs/automation_schedule.md を作成**

```bash
# 以下の内容でファイルを作成する (Write ツール使用)
```

ファイル内容は以下（Write ツールで `docs/automation_schedule.md` に書き込む）:

```markdown
# UMALOGI 自動化スケジュール一覧

> **最終更新**: 2026-05-23
> **ソースファイル**: `scripts/scheduler.py`

---

## 【Claude Codeへの絶対指示】

> 今後スケジュールや自動化スクリプトを改修する際は、必ずこの
> `docs/automation_schedule.md` も同時に書き換え、コードと仕様書の整合性を
> **100%保った状態でコミット**すること。スケジュール変更の PR は
> このファイルの更新を含まなければ Approve しないこと。

---

## 週次スケジュール タイムライン

| 曜日 | 時刻 | ジョブ名 | 内容 | 実装 |
|------|------|---------|------|------|
| 金 | 20:00 | `friday_job` | JVLink RACE同期(32bit) → WOOD同期(32bit) → マスタ同期(32bit) → **暫定予想生成**(64bit) → Discord暫定予想サマリー通知 | `scheduler.py:friday_job()` |
| 土 | 07:30 | `saturday_wood_sync` | JVLink WOOD同期（調教タイム・最新分） | `scheduler.py` |
| 日 | 07:30 | `sunday_wood_sync` | 同上 | `scheduler.py` |
| 土 | 08:30 | `auto_runner` | 当日全レース直前予想ループ起動（レース順に順次実行） | `scheduler.py:today_auto_runner()` |
| 日 | 08:30 | `auto_runner` | 同上 | `scheduler.py` |
| 土 | 09:00 | `win5_batch` | WIN5バッチ予測（締切前に実行） | `scheduler.py` |
| 日 | 09:00 | `win5_batch` | 同上 | `scheduler.py` |
| 土 | 10:30 | `note_article` | note AI厳選記事生成 → Discord転送 → (NOTE_DRAFT_AUTO_POST=1 時) note.com下書き自動保存 | `scheduler.py` |
| 日 | 10:30 | `note_article` | 同上 | `scheduler.py` |
| 土 | 13:00 | `umanity_post` | ウマニティ自動投稿（EV≥1.0 直前予想） | `scheduler.py` |
| 日 | 13:00 | `umanity_post` | 同上 | `scheduler.py` |
| 土 | 13:00 | `mid_sync` | レース中間結果同期（OPT_STORED） | `scheduler.py` |
| 土 | 15:30 | `mid_sync` | 同上 | `scheduler.py` |
| 日 | 13:00 | `mid_sync` | 同上 | `scheduler.py` |
| 日 | 15:30 | `mid_sync` | 同上 | `scheduler.py` |
| 土 | 17:30 | `post_race` | 払戻同期(32bit) + 的中評価 + Discord通知 + 増分学習 + バックアップ | `scheduler.py:post_race_job()` |
| 日 | 17:30 | `post_race` | 同上 | `scheduler.py` |
| 月 | 06:00 | `master_update` | マスタ差分更新 (DIFN/BLOD)(32bit) | `scheduler.py` |
| 月 | 07:00 | `weekly_retrain` | 週次全件再学習(64bit) | `scheduler.py` |
| 月 | 08:00 | `git_push` | GitHub 自動コミット・プッシュ | `scheduler.py` |

---

## 手動実行コマンド

```bash
# デーモン起動
python scripts/scheduler.py

# 即時実行（テスト用）
python scripts/scheduler.py --run-now friday
python scripts/scheduler.py --run-now auto_runner
python scripts/scheduler.py --run-now post_race --date 2024/01/06

# UI用JSONエクスポート（任意タイミングで実行可）
python web/generate_data.py
python web/generate_data.py --latest 50   # 直近50件のみ
```

---

## 環境変数一覧（スケジューラ関連）

| 変数名 | 用途 | 必須 |
|-------|------|------|
| `DISCORD_WEBHOOK_URL` | 買い目・結果・週次レポート | ✅ |
| `DISCORD_SYSTEM_WEBHOOK_URL` | システムログ・エラー | 推奨 |
| `DISCORD_WEBHOOK_HIT_FLASH` | **的中速報専用チャンネル** | 推奨 |
| `DISCORD_WEBHOOK_EV_ALERT` | EV≥1.5 激熱レース専用 (@everyone) | オプション |
| `DISCORD_WEBHOOK_NOTE_DRAFT` | note下書き転送 | オプション |
| `NOTE_DRAFT_AUTO_POST` | `1` でPlaywright自動投稿 | オプション |
| `NOTE_EMAIL` / `NOTE_PASSWORD` | note.com ログイン | NOTE自動投稿時必須 |

---

## 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2026-05-23 | 初版作成。scheduler.py のコメントより全スケジュールを整理。DISCORD_WEBHOOK_HIT_FLASH 追加。|
```

- [ ] **Step 4-2: docs/roadmap.md を作成**

ファイル内容（Write ツールで `docs/roadmap.md` に書き込む）:

```markdown
# UMALOGI ビジネスロードマップ

> **作成日**: 2026-05-23
> **方針**: 不労所得最大化 → NOTE販売 → 一般開放 → 地方競馬進出

---

## ロードマップ全体像

```
2026-Q2 (短期) ──→ 2026-Q3 (中期) ──→ 2026-Q4以降 (長期)
UI刷新            NOTE販売自動化       FastAPI一般開放
Discord完全化     X連動テキスト        地方競馬版AI
暫定予想安定化    地方競馬データ取込   共通ラッパー一元管理
```

---

## 短期ロードマップ (2026-Q2: 〜2026-06)

### フェーズ1: UI刷新・Discord完全リアル化 🔄

| タスク | ステータス | 完了日 |
|-------|----------|-------|
| RaceDetail 4サブタブ化（出馬表/結果/AI予想/的中結果） | 🟡対応中 | 2026-05-23予定 |
| AI予想タブにSNS投稿テキスト一発コピーボタン設置 | 🟡対応中 | 2026-05-23予定 |
| Discord 的中速報を専用チャンネル（HIT_FLASH）に分離 | 🟡対応中 | 2026-05-23予定 |
| Discord 直前予想通知に購入単価×点数を明記 | 🟡対応中 | 2026-05-23予定 |
| Discord 馬番全表示・軸推奨スマート表記 | 🟡対応中 | 2026-05-23予定 |
| generate_data.py で暫定予想をUIに安定反映 | 🟡対応中 | 2026-05-23予定 |
| docs/automation_schedule.md 永続化 | 🟡対応中 | 2026-05-23予定 |

### フェーズ2: NOTE販売 半自動化 (2026-06)

| タスク | ステータス | 完了日 |
|-------|----------|-------|
| UIへのMarkdownコピーボタン設置（note記事テンプレ） | 🔴未着手 | — |
| Selenium/Playwright 下書き自動保存（`NOTE_DRAFT_AUTO_POST=1`） | 🟡一部実装済 | — |
| NOTE記事の有料ライン（1レース目無料/以降有料）ロジック確立 | 🔴未着手 | — |
| 月間収益目標設定・KPI トラッキング開始 | 🔴未着手 | — |

---

## 中期ロードマップ (2026-Q3: 〜2026-09)

### フェーズ3: X連動テキスト生成・自動ポスト

| タスク | ステータス |
|-------|----------|
| X API v2 による予想ポスト自動生成（Claude Haiku 活用） | 🔴未着手 |
| NOTE記事公開 → X告知ポスト自動生成 | 🟡設計済 (`router.py:_generate_x_post()`) |
| X予想家シグナル取込 (`x_signals` テーブル) Phase A〜C | 🟡Phase B実装済 |

### フェーズ4: 地方競馬データ基盤

| タスク | ステータス |
|-------|----------|
| 地方競馬データソース調査（NARデータ / oi-nar.jp等） | 🔴未着手 |
| JRA/地方共通ラッパー設計（`src/scraper/common.py`） | 🔴未着手 |
| 地方競馬レース取得パイプライン実装 | 🔴未着手 |
| 地方競馬向けモデル訓練・バックテスト | 🔴未着手 |

---

## 長期ロードマップ (2026-Q4以降)

### フェーズ5: システム一般開放 (FastAPI + Next.js)

| タスク | ステータス |
|-------|----------|
| FastAPI バックエンド設計（認証・レート制限・課金） | 🔴未着手 |
| Next.js パブリック API クライアント実装 | 🔴未着手 |
| ユーザー管理（Supabase / Clerk 等検討） | 🔴未着手 |
| 無料プラン / プレミアムプラン ティア設計 | 🔴未着手 |
| ベータテスター募集・フィードバック収集 | 🔴未着手 |

### フェーズ6: 地方競馬版AI 創設

| タスク | ステータス |
|-------|----------|
| UI一元管理（JRA/地方 切替タブ） | 🔴未着手 |
| 地方競馬特有特徴量追加（騎手傾向・コース特性） | 🔴未着手 |
| Discord 地方競馬専用通知チャンネル | 🔴未着手 |

---

## KPI 目標

| 指標 | 現状 | 短期目標 | 中期目標 |
|-----|------|---------|---------|
| 単勝ROI | 691.5% (バックテスト) | 実運用200%+ | 実運用250%+ |
| 複勝ROI | 95.4% | 110%+ | 120%+ |
| NOTE月間収益 | ¥0 | ¥10,000+ | ¥50,000+ |
| 月間的中通知数 | — | 20件/月 | 50件/月 |

---

## 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2026-05-23 | 初版作成。短期/中期/長期の3フェーズロードマップを策定。|
```

- [ ] **Step 4-3: Commit**

```bash
git add docs/automation_schedule.md docs/roadmap.md
git commit -m "docs: automation_schedule.md と roadmap.md を新規作成（ビジネス拡大ロードマップ永続化）"
```

---

## Task 5: 最終確認・docs更新・完了コミット

- [ ] **Step 5-1: TypeScriptビルド最終確認**

```bash
cd web && npx tsc --noEmit 2>&1 | head -20
```
Expected: エラーなし

- [ ] **Step 5-2: docs更新（CLAUDE.md関連）**

以下のドキュメントに 2026-05-23 の変更履歴を追記:

`docs/4_ui_design.md` の更新履歴セクション:
```
| 2026-05-23 | RaceDetail を出馬表/レース結果/AI予想/的中結果の4サブタブに再編成。SNS投稿コピーボタン追加。影響ファイル: web/src/components/RaceDetail.tsx |
```

`docs/1_prediction_logic.md` の更新履歴セクション:
(Discord通知フォーマット変更)
```
| 2026-05-23 | Discord通知刷新: 的中速報チャンネル分離(HIT_FLASH)/購入単価明記/馬番全表示/軸推奨スマート表記。影響ファイル: src/notification/discord_notifier.py |
```

- [ ] **Step 5-3: roadmap.md の完了ステータスを更新**

`docs/roadmap.md` の各完了タスクを `🟡対応中` → `🟢完了` に更新。

- [ ] **Step 5-4: 最終コミット**

```bash
git add docs/4_ui_design.md docs/1_prediction_logic.md docs/roadmap.md
git commit -m "docs: 変更履歴追記・roadmap.md 完了ステータス更新"
```

---

## セルフレビュー結果

**スペックカバレッジ:**
- ✅ UI 4タブ化 (Task 2)
- ✅ SNSコピーボタン (Task 2-4)
- ✅ 的中速報チャンネル分離 (Task 3-1,2)
- ✅ 購入単価×点数明記 (Task 3-3)
- ✅ 馬番全表示バグ修正 (Task 3-4)
- ✅ 軸推奨スマート表記 (Task 3-4)
- ✅ 明日の暫定予想UIへの反映 (Task 1)
- ✅ automation_schedule.md (Task 4-1)
- ✅ roadmap.md (Task 4-2)

**placeholder scan:** TBD/TODO なし。全ステップにコード記載あり。

**型整合性:**
- `Tab` 型は Task 2-1 で定義し、その後のステップで一貫使用
- `buildSnsText` / `SnsShareButton` は同 Task 2-4 で定義・参照
- `_format_combo_card` は Task 3-4 で完全上書き（後方互換: `_summarize_combos` エイリアスは既存のまま）
