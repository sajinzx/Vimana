import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { post } from "../api";

const AIRPORTS = ["VIDP", "VABB", "VOBL", "VOMM", "VOHS", "VECC"];

export default function SimControls() {
  const [airport, setAirport] = useState("VIDP");
  const [vis, setVis] = useState(200);
  const [load, setLoad] = useState(1.5);
  const send = useMutation({ mutationFn: (body: Record<string, unknown>) => post("/sim/control", body) });

  return (
    <div>
      <div className="row" style={{ marginBottom: 12 }}>
        <select value={airport} onChange={(e) => setAirport(e.target.value)}>
          {AIRPORTS.map((a) => (
            <option key={a}>{a}</option>
          ))}
        </select>
        <label>
          Visibility (m){" "}
          <input type="number" value={vis} min={50} step={50} style={{ width: 90 }} onChange={(e) => setVis(+e.target.value)} />
        </label>
        <button onClick={() => send.mutate({ action: "set_fog", airport, visibility_m: vis, duration_min: 60 })}>
          Inject fog
        </button>
      </div>
      <div className="row" style={{ marginBottom: 12 }}>
        <label>
          Load factor{" "}
          <input type="number" value={load} min={0} step={0.25} style={{ width: 90 }} onChange={(e) => setLoad(+e.target.value)} />
        </label>
        <button onClick={() => send.mutate({ action: "set_load", airport, load_factor: load })}>Set load</button>
        <button onClick={() => send.mutate({ action: "reset" })}>Reset</button>
      </div>
      {send.isError && <div className="error">{(send.error as Error).message}</div>}
      {send.isSuccess && <div className="muted">Command sent.</div>}
    </div>
  );
}
