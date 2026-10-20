import { useQuery } from "@tanstack/react-query";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { get } from "../api";
import SimControls from "../components/SimControls";

interface Progress {
  name: string;
  batchId: number;
  ts: string;
  inputRowsPerSecond: number | null;
  processedRowsPerSecond: number | null;
  triggerExecutionMs: number | null;
  stateRows: number;
  maxOffsetsBehindLatest: number | null;
}

interface PipelineData {
  queries: Record<string, Progress[]>;
  dq: { valid?: number; invalid?: number; invalid_by_reason?: Record<string, number>; latency_ms_p50?: number; latency_ms_p95?: number };
  dlq_count: number;
  dlq_samples: { source: string; reason: string; raw: string }[];
}

const COLORS = ["#22d3ee", "#fbbf24", "#34d399", "#a78bfa", "#f472b6", "#60a5fa"];
const tip = { background: "#18223a", border: "1px solid #243049" };

function Chart({ data, field, title }: { data: PipelineData; field: keyof Progress; title: string }) {
  const names = Object.keys(data.queries);
  const rows = new Map<number, Record<string, number>>();
  names.forEach((n) =>
    data.queries[n].forEach((p, i) => {
      const row = rows.get(i) ?? { i };
      row[n] = Number(p[field] ?? 0);
      rows.set(i, row);
    }),
  );
  return (
    <div className="card">
      <h3>{title}</h3>
      <div style={{ height: 220 }}>
        <ResponsiveContainer>
          <LineChart data={[...rows.values()]}>
            <CartesianGrid stroke="#243049" />
            <XAxis dataKey="i" hide />
            <YAxis stroke="#8493ad" fontSize={11} />
            <Tooltip contentStyle={tip} />
            <Legend />
            {names.map((n, k) => (
              <Line key={n} dataKey={n} stroke={COLORS[k % COLORS.length]} dot={false} isAnimationActive={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default function Pipeline() {
  const { data } = useQuery({ queryKey: ["pipeline"], queryFn: () => get<PipelineData>("/pipeline"), refetchInterval: 5000 });
  const { data: health } = useQuery({
    queryKey: ["health"],
    queryFn: () => get<Record<string, unknown>>("/health"),
    refetchInterval: 10_000,
  });

  return (
    <div className="page">
      <h1>Pipeline health</h1>
      <div className="row" style={{ marginBottom: 16 }}>
        {health &&
          Object.entries(health).map(([k, v]) => (
            <span key={k} className={`badge ${v === false ? "red" : "green"}`}>
              {k}: {String(v)}
            </span>
          ))}
      </div>
      {!data || Object.keys(data.queries).length === 0 ? (
        <div className="card notice">Waiting for streaming progress events…</div>
      ) : (
        <div className="grid cols-2">
          <Chart data={data} field="inputRowsPerSecond" title="Input rows / s" />
          <Chart data={data} field="processedRowsPerSecond" title="Processed rows / s" />
          <Chart data={data} field="triggerExecutionMs" title="Batch duration (ms)" />
          <Chart data={data} field="maxOffsetsBehindLatest" title="Offsets behind latest" />
        </div>
      )}
      <div className="grid cols-2" style={{ marginTop: 16 }}>
        <div className="card">
          <h3>Data quality (J1)</h3>
          <div className="row">
            <span>Valid <b className="mono">{data?.dq.valid ?? "–"}</b></span>
            <span>Invalid <b className="mono">{data?.dq.invalid ?? "–"}</b></span>
            <span>Latency p50 <b className="mono">{data?.dq.latency_ms_p50 ?? "–"} ms</b></span>
            <span>p95 <b className="mono">{data?.dq.latency_ms_p95 ?? "–"} ms</b></span>
          </div>
          <div className="muted" style={{ marginTop: 8 }}>
            {Object.entries(data?.dq.invalid_by_reason ?? {}).map(([r, n]) => `${r}: ${n}`).join(" · ")}
          </div>
          <h3 style={{ marginTop: 16 }}>Dead-letter queue ({data?.dlq_count ?? 0})</h3>
          <table>
            <thead><tr><th>Reason</th><th>Source</th><th>Raw</th></tr></thead>
            <tbody>
              {(data?.dlq_samples ?? []).slice().reverse().slice(0, 8).map((s, i) => (
                <tr key={i}><td>{s.reason}</td><td>{s.source}</td><td className="mono muted">{String(s.raw).slice(0, 60)}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="card">
          <h3>Scenario controls (synthetic provider)</h3>
          <SimControls />
        </div>
      </div>
    </div>
  );
}
