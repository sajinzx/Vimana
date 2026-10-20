import { useQuery } from "@tanstack/react-query";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { get } from "../api";
import type { Prediction, TrackPoint } from "../types";

interface Detail {
  icao24: string;
  callsign: string | null;
  category: string | null;
  alt_ft: number | null;
  gs_kt: number | null;
  vr_fpm: number | null;
  phase: string | null;
  airport: string | null;
  holding: boolean;
  prediction: Prediction | null;
}

interface Props {
  icao24: string;
  track: TrackPoint[] | undefined;
  onClose: () => void;
}

export default function AircraftPanel({ icao24, track, onClose }: Props) {
  const { data, isError } = useQuery({
    queryKey: ["aircraft", icao24],
    queryFn: () => get<Detail>(`/aircraft/${icao24}`),
    refetchInterval: 5000,
  });

  const profile = (track ?? []).map((p) => ({
    t: new Date(p.ts).toISOString().slice(11, 16),
    alt: p.alt_ft != null ? Math.round(p.alt_ft) : null,
  }));

  return (
    <div className="card side-panel">
      <div className="row" style={{ justifyContent: "space-between" }}>
        <b className="mono" style={{ fontSize: 18 }}>
          {data?.callsign ?? icao24}
        </b>
        <button onClick={onClose}>×</button>
      </div>
      {isError && <div className="error">Aircraft no longer tracked.</div>}
      {data && (
        <>
          <div className="muted">
            {data.category ?? "–"} · {data.airport ?? "en route"} ·{" "}
            <span className={`badge ${data.holding ? "amber" : "green"}`}>{data.phase ?? "–"}</span>
          </div>
          <table style={{ marginTop: 10 }}>
            <tbody>
              <tr><th>Altitude</th><td className="mono">{data.alt_ft != null ? `${Math.round(data.alt_ft)} ft` : "–"}</td></tr>
              <tr><th>Ground speed</th><td className="mono">{data.gs_kt != null ? `${Math.round(data.gs_kt)} kt` : "–"}</td></tr>
              <tr><th>Vertical rate</th><td className="mono">{data.vr_fpm != null ? `${Math.round(data.vr_fpm)} fpm` : "–"}</td></tr>
              <tr>
                <th>Predicted extra time</th>
                <td className="mono">
                  {data.prediction?.pred_min != null ? `+${data.prediction.pred_min.toFixed(1)} min` : "–"}
                  {data.prediction?.actual_min != null && ` (actual +${data.prediction.actual_min.toFixed(1)})`}
                </td>
              </tr>
            </tbody>
          </table>
        </>
      )}
      <h3 style={{ marginTop: 16 }} className="muted">Altitude profile (60 min)</h3>
      <div style={{ height: 140 }}>
        <ResponsiveContainer>
          <LineChart data={profile}>
            <CartesianGrid stroke="#243049" />
            <XAxis dataKey="t" stroke="#8493ad" fontSize={10} />
            <YAxis stroke="#8493ad" fontSize={10} width={44} />
            <Tooltip contentStyle={{ background: "#18223a", border: "1px solid #243049" }} />
            <Line type="monotone" dataKey="alt" stroke="#22d3ee" dot={false} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
