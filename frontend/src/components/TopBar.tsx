import { NavLink } from "react-router-dom";
import { useLive } from "../store";
import KpiCard from "./KpiCard";

const LINKS: [string, string][] = [
  ["/", "Live"],
  ["/airports/VIDP", "Airports"],
  ["/analytics", "Analytics"],
  ["/model", "Model"],
  ["/pipeline", "Pipeline"],
];

export default function TopBar() {
  const kpis = useLive((s) => s.kpis);
  const connected = useLive((s) => s.connected);
  return (
    <header className="topbar">
      <span className="brand">VIMANA</span>
      <nav className="nav">
        {LINKS.map(([to, label]) => (
          <NavLink key={to} to={to} end={to === "/"} className={({ isActive }) => (isActive ? "active" : "")}>
            {label}
          </NavLink>
        ))}
      </nav>
      <div className="kpis">
        <KpiCard label="Tracked" value={kpis?.tracked} />
        <KpiCard label="Holding" value={kpis?.holding} />
        <KpiCard label="Msgs/s" value={kpis?.msgs_per_s != null ? kpis.msgs_per_s.toFixed(0) : undefined} />
        <KpiCard
          label="p95 latency"
          value={kpis?.e2e_latency_ms_p95 != null ? `${Math.round(kpis.e2e_latency_ms_p95)} ms` : undefined}
        />
        <span title={connected ? "live" : "disconnected"} className={`dot ${connected ? "on" : ""}`} />
      </div>
    </header>
  );
}
