import type { Metar } from "../types";

export default function MetarCard({ metar }: { metar: Metar }) {
  if (!metar.raw) return <div className="notice">No METAR yet</div>;
  return (
    <div>
      <div className="mono" style={{ marginBottom: 10 }}>
        {metar.raw}
      </div>
      <div className="row">
        <span>
          Vis <b>{metar.vis_m ?? "–"} m</b>
        </span>
        <span>
          Wind <b>{metar.wind_dir ?? "–"}° / {metar.wind_kt ?? "–"} kt</b>
        </span>
        <span>
          Ceiling <b>{metar.ceiling_ft ?? "–"} ft</b>
        </span>
        <span className={`badge ${metar.fog ? "red" : "green"}`}>{metar.fog ? "FOG" : metar.category ?? "OK"}</span>
      </div>
    </div>
  );
}
