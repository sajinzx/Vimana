import { useEffect } from "react";
import { Route, Routes } from "react-router-dom";
import AlertToasts from "./components/AlertToasts";
import TopBar from "./components/TopBar";
import Analytics from "./pages/Analytics";
import AirportDetail from "./pages/AirportDetail";
import LiveMap from "./pages/LiveMap";
import ModelPage from "./pages/ModelPage";
import Pipeline from "./pages/Pipeline";
import { startLiveSocket } from "./ws";

export default function App() {
  useEffect(() => startLiveSocket(), []);
  return (
    <div className="app">
      <TopBar />
      <div className="content">
        <Routes>
          <Route path="/" element={<LiveMap />} />
          <Route path="/airports/:icao" element={<AirportDetail />} />
          <Route path="/analytics" element={<Analytics />} />
          <Route path="/model" element={<ModelPage />} />
          <Route path="/pipeline" element={<Pipeline />} />
        </Routes>
      </div>
      <AlertToasts />
    </div>
  );
}
