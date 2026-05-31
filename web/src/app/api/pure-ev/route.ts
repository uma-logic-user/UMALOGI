import { NextResponse } from "next/server";
import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const DB_PATH = path.join(process.cwd(), "..", "data", "umalogi.db");
const BACKTEST_JSON = path.join(
  process.cwd(),
  "..",
  "data",
  "pure_ev_edge_backtest.json"
);

export const dynamic = "force-dynamic";

// CLAUDE.md §10.5: 文字化け検知（化けた文字列はレスポンスに含めない）
function isGarbled(s: string | null): boolean {
  if (!s) return false;
  return (
    /(\?[\x21-\x7e]){2,}/.test(s) ||
    /[｡-ﾟ]/.test(s) ||
    /[Ͱ-Ͽ]{2,}/.test(s)
  );
}
function clean(s: string | null): string {
  if (!s) return "";
  return isGarbled(s) ? "" : s;
}

interface BetRow {
  race_id: string;
  model_type: string;
  bet_type: string;
  confidence: number | null;
  expected_value: number | null;
  recommended_bet: number | null;
  notes: string | null;
  combination_json: string | null;
  created_at: string;
}

interface PerfRow {
  bet_type: string;
  n: number;
  hits: number;
  payout: number;
  profit: number;
  invest: number;
}

/**
 * 黒字化専用枠 Pure_EV_Edge の買い目と成績を独立して返す API。
 * 既存の本命/卍 とは完全分離（model_type LIKE 'Pure_EV_Edge%' のみ）。
 */
export function GET() {
  const db = new Database(DB_PATH, { readonly: true });
  try {
    // 直近の買い目（単複のみ）
    const bets = db
      .prepare(
        `SELECT p.race_id, p.model_type, p.bet_type, p.confidence,
                p.expected_value, p.recommended_bet, p.notes, p.combination_json,
                p.created_at
         FROM predictions p
         WHERE p.model_type LIKE 'Pure_EV_Edge%'
           AND COALESCE(p.is_superseded, 0) = 0
         ORDER BY p.created_at DESC
         LIMIT 200`
      )
      .all() as BetRow[];

    // 成績サマリー（券種別 ROI・的中率）— 実購入額(¥100×点数)ベース
    const perf = db
      .prepare(
        `SELECT p.bet_type AS bet_type,
                COUNT(*) AS n,
                SUM(COALESCE(pr.is_hit, 0)) AS hits,
                COALESCE(SUM(pr.payout), 0) AS payout,
                COALESCE(SUM(pr.profit), 0) AS profit,
                COALESCE(SUM(json_array_length(p.combination_json)) * 100, 0) AS invest
         FROM predictions p
         JOIN prediction_results pr ON pr.prediction_id = p.id
         WHERE p.model_type LIKE 'Pure_EV_Edge%'
         GROUP BY p.bet_type`
      )
      .all() as PerfRow[];

    const summary = perf.map((r) => ({
      bet_type: clean(r.bet_type),
      n: r.n,
      hits: r.hits,
      hit_rate: r.n > 0 ? Math.round((r.hits / r.n) * 1000) / 10 : 0,
      payout: r.payout,
      profit: r.profit,
      invest: r.invest,
      roi: r.invest > 0 ? Math.round((r.payout / r.invest) * 1000) / 10 : 0,
    }));

    const cleanBets = bets.map((b) => ({
      race_id: b.race_id,
      bet_type: clean(b.bet_type),
      confidence: b.confidence,
      expected_value: b.expected_value,
      recommended_bet: b.recommended_bet,
      notes: clean(b.notes),
      combination: b.combination_json
        ? (JSON.parse(b.combination_json) as number[][])
        : [],
      created_at: b.created_at,
    }));

    // 2年バックテスト結果（あれば添付）
    let backtest: unknown = null;
    try {
      if (fs.existsSync(BACKTEST_JSON)) {
        backtest = JSON.parse(fs.readFileSync(BACKTEST_JSON, "utf-8"));
      }
    } catch {
      backtest = null;
    }

    return NextResponse.json({
      variant: "Pure_EV_Edge",
      description: "黒字化専用枠（単勝・複勝のみ／EV>=1.15／1/10 Kelly／サーキットブレーカー）",
      bets: cleanBets,
      performance: summary,
      backtest,
    });
  } catch (e) {
    return NextResponse.json(
      { error: String(e), bets: [], performance: [] },
      { status: 500 }
    );
  } finally {
    db.close();
  }
}
