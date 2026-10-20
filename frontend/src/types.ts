export interface Aircraft {
  icao24: string;
  callsign: string | null;
  lat: number | null;
  lon: number | null;
  alt_ft: number | null;
  gs_kt: number | null;
  track: number | null;
  phase: string | null;
  airport: string | null;
  holding: boolean;
}

export interface Kpis {
  tracked: number;
  holding: number;
  msgs_per_s: number | null;
  e2e_latency_ms_p95: number | null;
}

export interface Alert {
  alert_id: string;
  kind: string;
  severity: string;
  airport: string;
  callsign?: string | null;
  icao24?: string;
  ts: number;
  message: string;
}

export interface Metar {
  raw: string | null;
  vis_m: number | null;
  wind_dir: number | null;
  wind_kt: number | null;
  ceiling_ft: number | null;
  fog: boolean;
  category: string | null;
}

export interface AirportSummary {
  icao: string;
  name: string;
  lat: number;
  lon: number;
  capacity_per_10min: number;
  metrics: Record<string, number | null>;
  congestion: number;
  band: "green" | "amber" | "red";
  metar: Metar;
  today?: {
    holds: number;
    go_arounds: number;
    diversions: number;
    landings: number;
    extra_fuel_kg: number;
    extra_co2_kg: number;
  };
}

export interface Prediction {
  icao24: string;
  entry_ts: number;
  airport: string | null;
  pred_min: number | null;
  model_version: string | null;
  actual_min: number | null;
  error_min: number | null;
}

export interface FlightEvent {
  airport: string;
  type: string;
  icao24: string | null;
  callsign: string | null;
  ts: number;
  details: Record<string, unknown>;
}

export interface TrackPoint {
  ts: number;
  lat: number | null;
  lon: number | null;
  alt_ft: number | null;
  gs_kt: number | null;
}
