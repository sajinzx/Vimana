import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { get } from "../api";
import CongestionGauge from "../components/CongestionGauge";
import MetarCard from "../components/MetarCard";
import type { AirportSummary, FlightEvent, Prediction } from "../types";

const AIRPORTS = ["VIDP", "VABB", "VOBL", "VOMM", "VOHS", "VECC"];
type History = Record<string, [number, number][]>;

function mergeSeries(h: History | undefined) {
  const rows = new Map<number, Record<string, number>>();
  for (const [name, pts] of Object.entries(h ?? {})) {
    for (const [ts, v] of pts) {
      const key = Math.round(ts / 60_000) * 60_000;
      rows.set(key, { ...(rows.get(key) ?? { ts: key }), [name]: v });
    }
  }
  return [...rows.values()].sort((a, b) => a.ts - b.ts);
}

const time = (ts: number) => new Date(ts).toISOString().slice(11, 16);

export default function AirportDetail() {
  const icao = (useParams().icao ?? "VIDP").toUpperCase();
  const { data: apt, isError } = useQuery({
    queryKey: ["airport", icao],
    queryFn: () => get<AirportSummary>(`/airports/${icao}`),
    refetchInterval: 10_000,
  });
  const { data: history } = useQuery({
    queryKey: ["history", icao],
    queryFn: () => get<History>(`/airports/${icao}/history?hours=24`),
    refetchInterval: 30_000,
  });
  const { data: preds } = useQuery({
    queryKey: ["preds", icao],
    queryFn: () => get<Prediction[]>(`/predictions?airport=${icao}&limit=20`),
    refetchInterval: 10_000,
  });
  const { data: events } = useQuery({
    queryKey: ["events", icao],
    queryFn: () => get<FlightEvent[]>(`/events?airport=${icao}&limit=15`),
    refetchInterval: 10_000,
  });
  const series = mergeSeries(history);

  return (
    <div className="page">
      <div className="row" style={{ marginBottom: 12 }}>
        <h1 style={{ margin: 0 }}>{apt?.name ?? icao}</h1>
        <span className="mono muted">{icao}</span>
        <div className="row" style={{ marginLeft: "auto" }}>
          {AIRPORTS.map((a) => (
            <Link key={a} to={`/airports/${a}`}>
              <button className={a === icao ? "active" : ""}>{a}</button>
            </Link>
          ))}
        </div>
      </div>
      {isError && <div className="notice error">Unknown airport.</div>}
      {apt && (
        <div className="grid" style={{ gap: 16 }}>
          <div className="grid cols-3">
            <div className="card">
              <h3>Congestion</h3>
              <CongestionGauge score={apt.congestion} band={apt.band} />
            </div>
            <div className="card" style={{ gridColumn: "span 2" }}>
              <h3>Latest METAR</h3>
              <MetarCard metar={apt.metar} />
            </div>
          </div>
          <div className="grid cols-3">
            <div className="card"><h3>Holds today</h3><b className="mono" style={{ fontSize: 28 }}>{apt.today?.holds ?? 0}</b></div>
            <div className="card"><h3>Go-arounds today</h3><b className="mono" style={{ fontSize: 28 }}>{apt.today?.go_arounds ?? 0}</b></div>
            <div className="card">
              <h3>Holding fuel / CO₂ today</h3>
              <b className="mono" style={{ fontSize: 22 }}>
                {Math.round(apt.today?.extra_fuel_kg ?? 0)} kg / {Math.round(apt.today?.extra_co2_kg ?? 0)} kg
              </b>
            </div>
          </div>
          <div className="card">
            <h3>Last 24 hours</h3>
            <div style={{ height: 280 }}>
              {series.length === 0 ? (
                <div className="notice">History fills in once the streaming job has run for a few minutes.</div>
              ) : (
                <ResponsiveContainer>
                  <LineChart data={series}>
                    <CartesianGrid stroke="#243049" />
                    <XAxis dataKey="ts" tickFormatter={time} stroke="#8493ad" fontSize={11} />
                    <YAxis yAxisId="l" stroke="#8493ad" fontSize={11} />
                    <YAxis yAxisId="r" orientation="right" stroke="#8493ad" fontSize={11} />
                    <Tooltip labelFormatter={time} contentStyle={{ background: "#18223a", border: "1px solid #243049" }} />
                    <Legend />
                    <Line yAxisId="l" dataKey="traffic_40nm" name="Traffic ≤40 NM" stroke="#22d3ee" dot={false} isAnimationActive={false} />
                    <Line yAxisId="l" dataKey="holds_15m" name="Holds (15 m)" stroke="#fbbf24" dot={false} isAnimationActive={false} />
                    <Line yAxisId="l" dataKey="arrivals_15m" name="Arrivals (15 m)" stroke="#34d399" dot={false} isAnimationActive={false} />
                    <Line yAxisId="r" dataKey="vis_m" name="Visibility (m)" stroke="#a78bfa" dot={false} isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
          <div className="grid cols-2">
            <div className="card">
              <h3>Inbound predictions</h3>
              <table>
                <thead><tr><th>Entered</th><th>Aircraft</th><th>Predicted</th><th>Actual</th><th>Error</th></tr></thead>
                <tbody>
                  {(preds ?? []).map((p) => (
                    <tr key={`${p.icao24}${p.entry_ts}`}>
                      <td className="mono">{time(p.entry_ts)}</td>
                      <td className="mono">{p.icao24}</td>
                      <td>+{p.pred_min?.toFixed(1)} min</td>
                      <td>{p.actual_min != null ? `+${p.actual_min.toFixed(1)}` : "–"}</td>
                      <td>{p.error_min != null ? p.error_min.toFixed(1) : "–"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {(preds ?? []).length === 0 && <div className="notice">No predictions yet.</div>}
            </div>
            <div className="card">
              <h3>Recent events</h3>
              <table>
                <thead><tr><th>Time</th><th>Event</th><th>Flight</th></tr></thead>
                <tbody>
                  {(events ?? []).map((e, i) => (
                    <tr key={i}>
                      <td className="mono">{time(e.ts)}</td>
                      <td>{e.type.replace(/_/g, " ")}</td>
                      <td className="mono">{e.callsign ?? e.icao24}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {(events ?? []).length === 0 && <div className="notice">No events yet.</div>}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
