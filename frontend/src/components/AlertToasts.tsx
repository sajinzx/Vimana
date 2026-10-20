import { useEffect } from "react";
import { useLive } from "../store";

export default function AlertToasts() {
  const alerts = useLive((s) => s.alerts);
  const dismiss = useLive((s) => s.dismissAlert);

  useEffect(() => {
    const timers = alerts.map((a) => window.setTimeout(() => dismiss(a.alert_id), 12_000));
    return () => timers.forEach(window.clearTimeout);
  }, [alerts, dismiss]);

  return (
    <div className="toasts">
      {alerts.map((a) => (
        <div key={a.alert_id} className={`toast ${a.severity}`} onClick={() => dismiss(a.alert_id)}>
          <b>{a.kind.replace(/_/g, " ")}</b> · {a.airport}
          <div className="muted">{a.message}</div>
        </div>
      ))}
    </div>
  );
}
