import { useQuery } from "@tanstack/react-query";
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { ApiError, get } from "../api";

type Row = Record<string, string | number | null>;

function useGold(name: string) {
  return useQuery({
    queryKey: ["gold", name],
    queryFn: () => get<Row[]>(`/analytics/${name}`),
    staleTime: 60_000,
    retry: false,
  });
}

function Missing({ error }: { error: unknown }) {
  const msg = error instanceof ApiError ? error.message : "unavailable";
  return <div className="notice">{msg}</div>;
}

function Heatmap({ rows }: { rows: Row[] }) {
  const airports = [...new Set(rows.map((r) => String(r.airport)))].sort();
  const hours = [...new Set(rows.map((r) => String(r.hour).slice(5, 13)))].sort().slice(-24);
  const val = new Map(rows.map((r) => [`${r.airport}|${String(r.hour).slice(5, 13)}`, Number(r.holding_min ?? 0)]));
  const max = Math.max(1, ...val.values());
  return (
    <div className="heat" style={{ gridTemplateColumns: `60px repeat(${hours.length}, 1fr)` }}>
      <div />
      {hours.map((h) => (
        <div key={h} style={{ color: "#8493ad", height: 14 }}>{h.slice(6)}</div>
      ))}
      {airports.flatMap((a) => [
        <div key={a} style={{ color: "#dbe4f3", justifyContent: "flex-start" }}>{a}</div>,
        ...hours.map((h) => {
          const v = val.get(`${a}|${h}`) ?? 0;
          return (
            <div key={`${a}${h}`} title={`${a} ${h}: ${v.toFixed(1)} min`} style={{ background: `rgba(251,191,36,${0.08 + (0.9 * v) / max})` }} />
          );
        }),
      ])}
    </div>
  );
}

function ClusterMap({ rows }: { rows: Row[] }) {
  const palette = ["#22d3ee", "#fbbf24", "#34d399", "#a78bfa", "#f472b6", "#60a5fa", "#fb923c", "#a3e635"];
  const airports = [...new Set(rows.map((r) => String(r.airport)))];
  return (
    <div className="grid cols-3">
      {airports.map((a) => {
        const paths = rows.filter((r) => r.airport === a).map((r) => ({ c: Number(r.cluster), p: JSON.parse(String(r.path_json)) as [number, number][] }));
        const pts = paths.flatMap((p) => p.p);
        const lats = pts.map((p) => p[0]);
        const lons = pts.map((p) => p[1]);
        const [x0, x1] = [Math.min(...lons), Math.max(...lons)];
        const [y0, y1] = [Math.min(...lats), Math.max(...lats)];
        const sx = (lon: number) => 10 + ((lon - x0) / (x1 - x0 || 1)) * 180;
        const sy = (lat: number) => 190 - ((lat - y0) / (y1 - y0 || 1)) * 180;
        return (
          <div key={a}>
            <div className="muted">{a}</div>
            <svg viewBox="0 0 200 200" style={{ width: "100%", background: "#0b1220", borderRadius: 8 }}>
              {paths.map((p, i) => (
                <polyline key={i} fill="none" stroke={palette[p.c % palette.length]} strokeWidth={2} points={p.p.map(([la, lo]) => `${sx(lo)},${sy(la)}`).join(" ")} />
              ))}
            </svg>
          </div>
        );
      })}
    </div>
  );
}

export default function Analytics() {
  const hourly = useGold("airport_hourly");
  const airlines = useGold("airline_stats");
  const weather = useGold("weather_impact");
  const efficiency = useGold("airport_efficiency");
  const clusters = useGold("approach_clusters");
  const daily = useGold("daily_kpis");

  return (
    <div className="page">
      <h1>Analytics</h1>
      <div className="grid">
        <div className="card">
          <h3>Holding minutes by airport and hour</h3>
          {hourly.data ? <Heatmap rows={hourly.data} /> : <Missing error={hourly.error} />}
        </div>
        <div className="grid cols-2">
          <div className="card">
            <h3>Airline league table</h3>
            {airlines.data ? (
              <table>
                <thead><tr><th>#</th><th>Airline</th><th>Flights</th><th>% held</th><th>Avg hold (min)</th></tr></thead>
                <tbody>
                  {[...airlines.data].sort((a, b) => Number(a.rank_by_holding) - Number(b.rank_by_holding)).slice(0, 12).map((r) => (
                    <tr key={String(r.airline)}>
                      <td>{r.rank_by_holding}</td><td className="mono">{r.airline}</td><td>{r.flights}</td>
                      <td>{Number(r.pct_held).toFixed(0)}%</td><td>{Number(r.avg_holding_min).toFixed(1)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : <Missing error={airlines.error} />}
          </div>
          <div className="card">
            <h3>Visibility vs additional terminal time</h3>
            {weather.data ? (
              <div style={{ height: 240 }}>
                <ResponsiveContainer>
                  <BarChart data={weather.data}>
                    <CartesianGrid stroke="#243049" />
                    <XAxis dataKey="visibility_bucket" stroke="#8493ad" fontSize={11} />
                    <YAxis stroke="#8493ad" fontSize={11} />
                    <Tooltip contentStyle={{ background: "#18223a", border: "1px solid #243049" }} />
                    <Bar dataKey="avg_additional_min" name="Avg extra min" fill="#fbbf24" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : <Missing error={weather.error} />}
          </div>
        </div>
        <div className="grid cols-2">
          <div className="card">
            <h3>Flight inefficiency by airport</h3>
            {efficiency.data ? (
              <table>
                <thead><tr><th>Airport</th><th>Flights</th><th>Extra distance</th></tr></thead>
                <tbody>
                  {[...efficiency.data].sort((a, b) => Number(b.inefficiency_pct) - Number(a.inefficiency_pct)).map((r) => (
                    <tr key={String(r.airport)}><td className="mono">{r.airport}</td><td>{r.flights}</td><td>{Number(r.inefficiency_pct).toFixed(1)}%</td></tr>
                  ))}
                </tbody>
              </table>
            ) : <Missing error={efficiency.error} />}
          </div>
          <div className="card">
            <h3>Daily KPIs</h3>
            {daily.data ? (
              <div style={{ height: 240 }}>
                <ResponsiveContainer>
                  <LineChart data={[...daily.data].sort((a, b) => String(a.day).localeCompare(String(b.day)))}>
                    <CartesianGrid stroke="#243049" />
                    <XAxis dataKey="day" stroke="#8493ad" fontSize={11} />
                    <YAxis stroke="#8493ad" fontSize={11} />
                    <Tooltip contentStyle={{ background: "#18223a", border: "1px solid #243049" }} />
                    <Line dataKey="holds" stroke="#fbbf24" isAnimationActive={false} />
                    <Line dataKey="go_arounds" stroke="#f87171" isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : <Missing error={daily.error} />}
          </div>
        </div>
        <div className="card">
          <h3>Approach path clusters</h3>
          {clusters.data ? <ClusterMap rows={clusters.data} /> : <Missing error={clusters.error} />}
        </div>
      </div>
    </div>
  );
}
