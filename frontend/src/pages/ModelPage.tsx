import { useQuery } from "@tanstack/react-query";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis } from "recharts";
import { get } from "../api";
import type { Prediction } from "../types";

interface Metrics {
  mae: number;
  rmse: number;
  r2: number;
}

interface ModelInfo {
  version: string;
  rolling_mae: number | null;
  recent_errors: Prediction[];
  metrics: {
    trained_at: number;
    train_rows: number;
    test_rows: number;
    improvement_over_baseline_pct: number;
    gbt: Metrics;
    linear_regression: Metrics;
    baseline_median: Metrics;
    feature_importances: Record<string, number>;
    test_predictions: { actual: number; predicted: number }[];
    params: Record<string, number>;
  } | null;
}

const tip = { background: "#18223a", border: "1px solid #243049" };

export default function ModelPage() {
  const { data } = useQuery({ queryKey: ["model"], queryFn: () => get<ModelInfo>("/model"), refetchInterval: 15_000 });
  const m = data?.metrics;

  const compare = m
    ? [
        { name: "Per-airport median", mae: m.baseline_median.mae, rmse: m.baseline_median.rmse },
        { name: "Linear regression", mae: m.linear_regression.mae, rmse: m.linear_regression.rmse },
        { name: "GBT", mae: m.gbt.mae, rmse: m.gbt.rmse },
      ]
    : [];
  const importances = Object.entries(m?.feature_importances ?? {})
    .slice(0, 12)
    .map(([name, v]) => ({ name, v: +v.toFixed(3) }));

  return (
    <div className="page">
      <h1>Delay model</h1>
      <div className="row" style={{ marginBottom: 16 }}>
        <span className="badge green">version {data?.version ?? "–"}</span>
        {m && <span className="muted">trained {new Date(m.trained_at).toLocaleString()} · {m.train_rows} train / {m.test_rows} test rows</span>}
        {m && <b>{m.improvement_over_baseline_pct.toFixed(1)}% lower MAE than per-airport median</b>}
        {data?.rolling_mae != null && <span>Live MAE (last {data.recent_errors.length}): <b>{data.rolling_mae.toFixed(2)} min</b></span>}
      </div>
      {!m ? (
        <div className="card notice">
          No trained model yet. Predictions use the per-airport median baseline. Run <span className="mono">make seed</span> then{" "}
          <span className="mono">make train</span>.
        </div>
      ) : (
        <div className="grid cols-2">
          <div className="card">
            <h3>MAE and RMSE (minutes, held-out last 20% of time)</h3>
            <div style={{ height: 260 }}>
              <ResponsiveContainer>
                <BarChart data={compare}>
                  <CartesianGrid stroke="#243049" />
                  <XAxis dataKey="name" stroke="#8493ad" fontSize={11} />
                  <YAxis stroke="#8493ad" fontSize={11} />
                  <Tooltip contentStyle={tip} />
                  <Bar dataKey="mae" fill="#22d3ee" name="MAE" />
                  <Bar dataKey="rmse" fill="#a78bfa" name="RMSE" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div className="card">
            <h3>Feature importance</h3>
            <div style={{ height: 260 }}>
              <ResponsiveContainer>
                <BarChart data={importances} layout="vertical" margin={{ left: 60 }}>
                  <CartesianGrid stroke="#243049" />
                  <XAxis type="number" stroke="#8493ad" fontSize={11} />
                  <YAxis type="category" dataKey="name" stroke="#8493ad" fontSize={11} width={110} />
                  <Tooltip contentStyle={tip} />
                  <Bar dataKey="v" fill="#fbbf24" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div className="card">
            <h3>Predicted vs actual (test set)</h3>
            <div style={{ height: 260 }}>
              <ResponsiveContainer>
                <ScatterChart>
                  <CartesianGrid stroke="#243049" />
                  <XAxis type="number" dataKey="actual" name="actual" stroke="#8493ad" fontSize={11} />
                  <YAxis type="number" dataKey="predicted" name="predicted" stroke="#8493ad" fontSize={11} />
                  <Tooltip contentStyle={tip} />
                  <Scatter data={m.test_predictions} fill="#22d3ee" />
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div className="card">
            <h3>Metrics</h3>
            <table>
              <thead><tr><th>Model</th><th>MAE</th><th>RMSE</th><th>R²</th></tr></thead>
              <tbody>
                {([["GBT", m.gbt], ["Linear regression", m.linear_regression], ["Per-airport median", m.baseline_median]] as [string, Metrics][]).map(([n, x]) => (
                  <tr key={n}><td>{n}</td><td>{x.mae.toFixed(2)}</td><td>{x.rmse.toFixed(2)}</td><td>{x.r2.toFixed(2)}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
