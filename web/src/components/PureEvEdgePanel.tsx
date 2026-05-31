"use client";

import { useState, useEffect } from "react";

interface PerfRow {
  bet_type: string;
  n: number;
  hits: number;
  hit_rate: number;
  payout: number;
  profit: number;
  invest: number;
  roi: number;
}

interface BetRow {
  race_id: string;
  bet_type: string;
  confidence: number | null;
  expected_value: number | null;
  recommended_bet: number | null;
  notes: string;
  combination: number[][];
  created_at: string;
}

interface BacktestStratSummary {
  n_bets: number;
  n_hits: number;
  hit_rate_pct: number;
  invest: number;
  payout: number;
  net: number;
  roi_pct: number;
  max_drawdown: number;
}

interface BacktestData {
  params?: { period?: string[]; ev_threshold?: number; kelly_fraction?: number };
  flat?: BacktestStratSummary;
  kelly?: BacktestStratSummary;
}

interface ApiResp {
  variant: string;
  description: string;
  bets: BetRow[];
  performance: PerfRow[];
  backtest: BacktestData | null;
}

const CARD = "#121829";
const BORDER = "1px solid #1c2333";

export default function PureEvEdgePanel() {
  const [data, setData] = useState<ApiResp | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/pure-ev")
      .then((r) => r.json())
      .then((d) => {
        setData(d);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  if (loading)
    return <div style={{ padding: 20, color: "#8b95a7" }}>読み込み中…</div>;
  if (!data)
    return (
      <div style={{ padding: 20, color: "#ff6b6b" }}>
        データ取得に失敗しました
      </div>
    );

  const bt = data.backtest;

  return (
    <div style={{ padding: "20px" }}>
      <h2 style={{ color: "#e8ecf4", fontSize: "20px", marginBottom: "6px" }}>
        💎 Pure_EV_Edge（黒字化専用枠）
      </h2>
      <p style={{ color: "#8b95a7", fontSize: "13px", marginBottom: "20px" }}>
        {data.description}
      </p>

      {/* 2年バックテスト結果 */}
      {bt && (bt.flat || bt.kelly) && (
        <section style={{ marginBottom: "28px" }}>
          <h3 style={{ color: "#5b9dff", fontSize: "15px", marginBottom: "10px" }}>
            2年間ウォークフォワード・バックテスト
            {bt.params?.period
              ? `（${bt.params.period[0]} 〜 ${bt.params.period[1]}）`
              : ""}
          </h3>
          <div style={{ display: "flex", gap: "16px", flexWrap: "wrap" }}>
            {(["flat", "kelly"] as const).map((k) => {
              const s = bt[k];
              if (!s) return null;
              const label = k === "flat" ? "フラット ¥100/点" : "1/10 Kelly";
              const ok = s.roi_pct >= 100;
              return (
                <div
                  key={k}
                  style={{
                    background: CARD,
                    border: BORDER,
                    borderRadius: "10px",
                    padding: "16px",
                    minWidth: "240px",
                  }}
                >
                  <div style={{ color: "#e8ecf4", fontWeight: 600 }}>{label}</div>
                  <div
                    style={{
                      color: ok ? "#4ade80" : "#ff6b6b",
                      fontSize: "26px",
                      fontWeight: 700,
                      margin: "6px 0",
                    }}
                  >
                    ROI {s.roi_pct}%
                  </div>
                  <div style={{ color: "#8b95a7", fontSize: "13px", lineHeight: 1.7 }}>
                    買い {s.n_bets.toLocaleString()}点 / 的中 {s.hit_rate_pct}%<br />
                    純損益 ¥{s.net.toLocaleString()}<br />
                    最大DD ¥{s.max_drawdown.toLocaleString()}
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      )}

      {/* 本番確定実績サマリー */}
      <section style={{ marginBottom: "28px" }}>
        <h3 style={{ color: "#5b9dff", fontSize: "15px", marginBottom: "10px" }}>
          本番確定実績（券種別）
        </h3>
        {data.performance.length === 0 ? (
          <div style={{ color: "#8b95a7", fontSize: "13px" }}>
            確定実績はまだありません
          </div>
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
            <thead>
              <tr style={{ color: "#8b95a7", textAlign: "left" }}>
                <th style={{ padding: "6px 8px" }}>券種</th>
                <th style={{ padding: "6px 8px" }}>点数</th>
                <th style={{ padding: "6px 8px" }}>的中率</th>
                <th style={{ padding: "6px 8px" }}>投資</th>
                <th style={{ padding: "6px 8px" }}>払戻</th>
                <th style={{ padding: "6px 8px" }}>ROI</th>
              </tr>
            </thead>
            <tbody>
              {data.performance.map((p) => (
                <tr key={p.bet_type} style={{ color: "#e8ecf4", borderTop: BORDER }}>
                  <td style={{ padding: "6px 8px" }}>{p.bet_type}</td>
                  <td style={{ padding: "6px 8px" }}>{p.n}</td>
                  <td style={{ padding: "6px 8px" }}>{p.hit_rate}%</td>
                  <td style={{ padding: "6px 8px" }}>¥{p.invest.toLocaleString()}</td>
                  <td style={{ padding: "6px 8px" }}>¥{p.payout.toLocaleString()}</td>
                  <td
                    style={{
                      padding: "6px 8px",
                      color: p.roi >= 100 ? "#4ade80" : "#ff6b6b",
                      fontWeight: 600,
                    }}
                  >
                    {p.roi}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {/* 直近の買い目 */}
      <section>
        <h3 style={{ color: "#5b9dff", fontSize: "15px", marginBottom: "10px" }}>
          直近の黒字化専用買い目（単複のみ）
        </h3>
        {data.bets.length === 0 ? (
          <div style={{ color: "#8b95a7", fontSize: "13px" }}>
            現在、EV≥1.15 を満たす買い目はありません
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
            {data.bets.slice(0, 50).map((b, i) => (
              <div
                key={`${b.race_id}-${b.bet_type}-${i}`}
                style={{
                  background: CARD,
                  border: BORDER,
                  borderRadius: "8px",
                  padding: "10px 14px",
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <div style={{ color: "#e8ecf4", fontSize: "13px" }}>
                  <span style={{ color: "#5b9dff" }}>{b.race_id}</span> ・{" "}
                  <strong>{b.bet_type}</strong> ・ {b.notes}
                </div>
                <div style={{ color: "#8b95a7", fontSize: "12px" }}>
                  EV {b.expected_value?.toFixed(2)} / 推奨 ¥
                  {(b.recommended_bet ?? 0).toLocaleString()}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
