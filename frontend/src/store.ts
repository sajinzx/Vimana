import { create } from "zustand";
import type { Aircraft, Alert, Kpis, Prediction } from "./types";

interface LiveState {
  connected: boolean;
  aircraft: Aircraft[];
  kpis: Kpis | null;
  alerts: Alert[];
  lastPrediction: Prediction | null;
  selected: string | null;
  setConnected: (v: boolean) => void;
  setSnapshot: (aircraft: Aircraft[], kpis: Kpis) => void;
  pushAlert: (a: Alert) => void;
  dismissAlert: (id: string) => void;
  setPrediction: (p: Prediction) => void;
  select: (icao24: string | null) => void;
}

export const useLive = create<LiveState>((set) => ({
  connected: false,
  aircraft: [],
  kpis: null,
  alerts: [],
  lastPrediction: null,
  selected: null,
  setConnected: (connected) => set({ connected }),
  setSnapshot: (aircraft, kpis) => set({ aircraft, kpis }),
  pushAlert: (a) =>
    set((s) =>
      s.alerts.some((x) => x.alert_id === a.alert_id)
        ? s
        : { alerts: [...s.alerts.slice(-4), a] },
    ),
  dismissAlert: (id) => set((s) => ({ alerts: s.alerts.filter((a) => a.alert_id !== id) })),
  setPrediction: (lastPrediction) => set({ lastPrediction }),
  select: (selected) => set({ selected }),
}));
