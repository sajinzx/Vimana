import { useLive } from "./store";

let socket: WebSocket | null = null;

/** Open the live socket with exponential backoff reconnects (1 s -> 10 s). */
export function startLiveSocket(): () => void {
  let stopped = false;
  let delay = 1000;
  let timer: number | undefined;

  const open = () => {
    if (stopped) return;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws/live`);
    socket = ws;
    ws.onopen = () => {
      delay = 1000;
      useLive.getState().setConnected(true);
    };
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      const st = useLive.getState();
      if (msg.type === "snapshot") st.setSnapshot(msg.aircraft, msg.kpis);
      else if (msg.type === "alert") st.pushAlert(msg.alert);
      else if (msg.type === "prediction") st.setPrediction(msg.prediction);
    };
    ws.onclose = () => {
      useLive.getState().setConnected(false);
      if (stopped) return;
      timer = window.setTimeout(open, delay);
      delay = Math.min(delay * 2, 10000);
    };
  };
  open();

  return () => {
    stopped = true;
    window.clearTimeout(timer);
    socket?.close();
  };
}

export function subscribeAirport(airport: string | null): void {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ subscribe: { airport } }));
  }
}
