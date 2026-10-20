import { H3HexagonLayer } from "@deck.gl/geo-layers";
import { IconLayer, PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { MapboxOverlay } from "@deck.gl/mapbox";
import { useQuery } from "@tanstack/react-query";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { get } from "../api";
import AircraftPanel from "../components/AircraftPanel";
import { useLive } from "../store";
import type { Aircraft, AirportSummary, TrackPoint } from "../types";

const STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
const PLANE =
  "data:image/svg+xml;charset=utf-8," +
  encodeURIComponent(
    '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 24 24"><path fill="white" d="M21 16v-2l-8-5V3.5a1.5 1.5 0 0 0-3 0V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5z"/></svg>',
  );
const BAND: Record<string, [number, number, number]> = {
  green: [52, 211, 153],
  amber: [251, 191, 36],
  red: [248, 113, 113],
};
const NM_M = 1852;

type RGB = [number, number, number];

function colour(a: Aircraft): RGB {
  if (a.holding || a.phase === "HOLDING") return [251, 191, 36];
  if (a.phase === "GO_AROUND") return [248, 113, 113];
  if (a.phase === "GROUND") return [100, 116, 139];
  if (a.phase === "APPROACH" || a.phase === "DESCENT") return [34, 211, 238];
  return [170, 180, 195];
}

export default function LiveMap() {
  const container = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const overlayRef = useRef<MapboxOverlay | null>(null);
  const navigate = useNavigate();
  const aircraft = useLive((s) => s.aircraft);
  const selected = useLive((s) => s.selected);
  const select = useLive((s) => s.select);
  const [density, setDensity] = useState(false);
  const [pulse, setPulse] = useState(0);

  const { data: airports } = useQuery({
    queryKey: ["airports"],
    queryFn: () => get<AirportSummary[]>("/airports"),
    refetchInterval: 10_000,
  });
  const { data: cells } = useQuery({
    queryKey: ["density"],
    queryFn: () => get<{ h3: string; count: number }[]>("/airspace/density?minutes=5"),
    enabled: density,
    refetchInterval: 15_000,
  });
  const { data: track } = useQuery({
    queryKey: ["track", selected],
    queryFn: () => get<TrackPoint[]>(`/aircraft/${selected}/track?minutes=60`),
    enabled: !!selected,
    refetchInterval: 10_000,
  });

  useEffect(() => {
    const t = window.setInterval(() => setPulse((p) => (p + 1) % 2), 600);
    return () => window.clearInterval(t);
  }, []);

  useEffect(() => {
    if (!container.current) return;
    const map = new maplibregl.Map({
      container: container.current,
      style: STYLE,
      center: [78.5, 21],
      zoom: 4.3,
      attributionControl: { compact: true },
    });
    const overlay = new MapboxOverlay({ interleaved: false, layers: [] });
    map.addControl(overlay as unknown as maplibregl.IControl);
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "bottom-left");
    mapRef.current = map;
    overlayRef.current = overlay;
    return () => {
      map.remove();
      mapRef.current = null;
      overlayRef.current = null;
    };
  }, []);

  useEffect(() => {
    const layers: unknown[] = [];
    if (density && cells) {
      const max = Math.max(1, ...cells.map((c) => c.count));
      layers.push(
        new H3HexagonLayer<{ h3: string; count: number }>({
          id: "density",
          data: cells,
          getHexagon: (d) => d.h3,
          getFillColor: (d) => [34, 211, 238, 40 + (160 * d.count) / max],
          extruded: false,
          pickable: false,
        }),
      );
    }
    if (airports) {
      layers.push(
        new ScatterplotLayer<AirportSummary>({
          id: "terminal-rings",
          data: airports,
          getPosition: (d) => [d.lon, d.lat],
          getRadius: 40 * NM_M,
          stroked: true,
          filled: false,
          getLineColor: (d) => [...BAND[d.band], 120] as [number, number, number, number],
          lineWidthMinPixels: 1,
        }),
        new ScatterplotLayer<AirportSummary>({
          id: "airport-markers",
          data: airports,
          getPosition: (d) => [d.lon, d.lat],
          getRadius: 9000,
          radiusMinPixels: 7,
          getFillColor: (d) => [...BAND[d.band], 230] as [number, number, number, number],
          pickable: true,
          onClick: ({ object }) => object && navigate(`/airports/${object.icao}`),
        }),
      );
    }
    if (track && track.length > 1) {
      layers.push(
        new PathLayer<{ path: [number, number][] }>({
          id: "track",
          data: [
            {
              path: track
                .filter((p) => p.lat != null && p.lon != null)
                .map((p) => [p.lon as number, p.lat as number] as [number, number]),
            },
          ],
          getPath: (d) => d.path,
          getColor: [34, 211, 238, 200],
          widthMinPixels: 2,
        }),
      );
    }
    layers.push(
      new IconLayer<Aircraft>({
        id: "aircraft",
        data: aircraft.filter((a) => a.lat != null && a.lon != null),
        getPosition: (a) => [a.lon as number, a.lat as number],
        getIcon: () => ({ url: PLANE, width: 64, height: 64, mask: true }),
        getAngle: (a) => -(a.track ?? 0),
        getSize: (a) => (a.holding ? (pulse ? 30 : 22) : 20),
        getColor: (a) => colour(a),
        sizeUnits: "pixels",
        pickable: true,
        onClick: ({ object }) => object && select(object.icao24),
        updateTriggers: { getSize: [pulse], getColor: [aircraft] },
      }),
    );
    overlayRef.current?.setProps({ layers: layers as never });
  }, [aircraft, airports, cells, density, track, pulse, navigate, select]);

  return (
    <div className="map-wrap">
      <div ref={container} className="map" />
      <div className="map-toolbar">
        <button className={density ? "active" : ""} onClick={() => setDensity((d) => !d)}>
          Density
        </button>
      </div>
      {selected && <AircraftPanel icao24={selected} track={track} onClose={() => select(null)} />}
    </div>
  );
}
