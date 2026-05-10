# UMALOGI UI 設計書（Discord / ダッシュボード）

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-10 | 初版作成。Discord 3セクション Embed レイアウト・Next.js ダッシュボード仕様記述 |
| 2026-05-10 | Hit Flash（的中速報）追加: `fetch_race_result.py:_send_hit_flash()` — 評価完了直後に予想チャンネルへ Embed 送信。的中あり=🎉予想ch/なし=🏁システムch |
| 2026-05-11 | PWA化: manifest.json / Service Worker / SwRegister.tsx / offline.html / アイコン4サイズ。影響: `web/public/` 全体・`web/src/app/layout.tsx` |
| 2026-05-11 | モバイルアクセス基盤: Cloudflare Named Tunnel セットアップ (`setup_named_tunnel.py`) / Windows自動起動 (`install_tunnel_service.ps1`) / Basic Auth Middleware (`web/src/middleware.ts`) |

---

## 1. Discord 通知設計

### 1-1. チャンネル構成

| 環境変数 | 用途 |
|---------|------|
| `DISCORD_WEBHOOK_URL` | **予想チャンネル**: 直前予想・結果・週次レポート |
| `DISCORD_SYSTEM_WEBHOOK_URL` | **システムチャンネル**: 起動/停止・エラー・自己修復ログ |

SYSTEM チャンネル未設定時は WEBHOOK_URL へ fallback。

---

### 1-2. 直前予想 Embed（3セクション分離形式）

**実装**: `src/notification/discord_notifier.py` — `notify_prerace_result()`

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 🏇 東京11R  ＮＨＫマイルカップ
  最大EV: 2.34  推奨投資合計: ¥4,500
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🟦 __ALPHA 予想  (期待値特化)__
   ​
🔥 三連複  EV=2.34  ¥1,500
  ▶ 軸: 5番 ダイヤモンドノット
    相手: 3番 / 9番
    計3点

🔥 複勝  EV=1.82  ¥1,000
  ⬛ 5番 ダイヤモンドノット

🟩 __卍 予想  (回収率特化)__
   ​
🔥 三連単  EV=1.95  ¥600
  ▶ 5番 → 9番 → 3番
  ▶ 5番 → 3番 → 9番
    (+1組)

🟥 __本命 予想  (勝率特化)__
   ​
🔥 馬連  EV=1.45  ¥800
  ⬛ 5番 ダイヤモンドノット
  ⬛ 9番 カヴァレリッツォ
```

**Embed カラーコード**:
- EV >= 3.0: `0xFFD700` (ゴールド / JACKPOT)
- EV >= 1.5: `0xFF6B35` (オレンジ / BIG)
- EV >= 0.0: `0x4ECDC4` (ティール / NORMAL)

**カードフォーマット** (`_format_combo_card`):
- 単勝/複勝: `⬛ N番 馬名`
- 三連複: `▶ 軸: N番 馬名\n  相手: A番 / B番\n  計X点`
- 三連単: `▶ A番 → B番 → C番\n  (+N組)`

**表示件数上限** (per section):
- ALPHA: 3件
- 卍: 3件
- 本命: 3件
- Oracle/HitFocus: 2件 (オプション表示)

**スキップ条件**: 全モデル EV <= 0 の場合、Discord 通知を送信しない。

---

### 1-3. Hit Flash（的中速報）— レース単位リアルタイム通知

**実装**: `scripts/fetch_race_result.py:_send_hit_flash(result, race_name)`  
**送信先**: 予想チャンネル (`DISCORD_WEBHOOK_URL`)  
**タイミング**: `fetch_single_race()` 内の `Evaluator.evaluate_race()` 完了直後

```
🎉 的中速報！  東京 11R ＮＨＫマイルカップ
**三連複**  5-9-3  ¥28,400  (投資¥1,500 / 利益+¥26,900)
**複勝**    5      ¥380    (投資¥500 / 利益-¥120)
─────────────────────────────────────
投資合計 ¥2,000  払戻合計 ¥28,780  ROI 1439.0%
```

外れた場合:
```
🏁 完走速報  東京 11R ＮＨＫマイルカップ
的中なし
─────────────────────────────────────
投資合計 ¥3,500
```

**カラー閾値** (Embed color):
- 払戻合計 ≥ ¥100,000: `0xFF4500` (赤橙 / 万馬券)
- 払戻合計 ≥ ¥10,000: `0xFFD700` (金 / 高配当)
- 払戻合計 < ¥10,000: `0x43B581` (緑 / 通常)
- 外れ: `0x555555` (グレー)

---

### 1-4. 結果速報 / 的中サマリー

**実装**: `notify_hit_summary()` / `notify_ror_warning()`

```
✅ 的中！ 東京11R ＮＨＫマイルカップ
  三連複 5-9-3: ¥28,400 (EV=1.89 → 実際2.52)
  損益: +¥26,900

⚠️ ROI 警告: 直近20件の回収率 68.3% (<80% 閾値)
```

---

### 1-4. システム通知（SYSTEM チャンネル）

| イベント | メッセージ例 |
|---------|------------|
| 起動 | `🚀 [UMALOGI] 週次オートパイロット 起動` |
| heartbeat (毎時) | `💚 [heartbeat] scheduler 正常稼働中` |
| 自己修復発動 | `⚠️ [自己修復] メタデータ異常を検知: 3レース → repair 実行` |
| エラー/クラッシュ | `❌ [エラー] prerace 失敗: {race_id}` |
| 週次スリープ | `💤 [スリープ] 次の起動: 2026-05-15 20:00 (金曜夜間バッチ)` |

---

## 2. Next.js ダッシュボード設計

**ディレクトリ**: `web/`  
**フレームワーク**: Next.js (App Router)  
**スタイル**: Tailwind CSS (ダークテーマ)  
**データ形式**: 静的 JSON (`web/src/data/`)

### 2-1. データファイル構成

```
web/src/data/
  races.json              # 全レース一覧 (meta情報 + predictions summary)
  races/
    {race_id}.json        # レース別詳細 (全モデルの買い目 + 結果)
```

**生成**: `web/generate_data.py` で `umalogi.db` から生成  
**更新タイミング**: 各 prerace/postrace 完了後 / 週次バッチ後

### 2-2. ページ構成

| ページ | パス | 説明 |
|-------|------|------|
| トップ | `/` | 本日のレース一覧・予想サマリー |
| レース詳細 | `/race/[race_id]` | 3モデル予想・買い目・結果 |
| 予想パネル | `PredictionsPanel` コンポーネント | レース別の予想一覧 |

### 2-3. PredictionsPanel コンポーネント

**ファイル**: `web/src/components/PredictionsPanel.tsx`

表示内容:
- モデルタイプ別タブ (ALPHA / 卍 / 本命)
- 買い目テーブル (馬番・馬名・EV・推奨投資額)
- 的中/外れ結果バッジ (postrace 後)
- 損益サマリー

---

## 3. 外部アクセス

### 3-1. ローカル開発サーバー

```bash
cd web && npm run dev   # http://localhost:3000
```

### 3-2. 本番公開 (予定)

- Vercel または GitHub Pages へのデプロイ
- データは静的 JSON を配信 (DB は直接公開しない)
- トンネル経由のローカル公開: `scripts/start_tunnel.py` (ngrok/cloudflared)

---

## 4. note / SNS 連携

| スクリプト | 出力 |
|-----------|------|
| `scripts/generate_note_article.py` | レース別 note 記事 (Markdown → HTML) |
| `scripts/generate_sns_post.py` | X (Twitter) 投稿文 (パターンA/B) |
| `scripts/generate_result_card.py` | 的中カード画像 (Pillow 製) |

**生成条件**:
- 注目レース (重賞・G1 or EV >= 5.0) の prerace 完了後に記事を先行生成
- 的中時に結果カード画像を自動生成して Discord に添付
