const COLORS = { green: "var(--green)", amber: "var(--amber)", red: "var(--red)" };

interface Props {
  score: number;
  band: "green" | "amber" | "red";
}

export default function CongestionGauge({ score, band }: Props) {
  return (
    <div className="gauge">
      <div className="value" style={{ color: COLORS[band] }}>
        {Math.round(score)}
      </div>
      <div className="bar">
        <div style={{ width: `${Math.min(score, 100)}%`, background: COLORS[band] }} />
      </div>
      <div className="muted" style={{ marginTop: 6 }}>
        congestion score
      </div>
    </div>
  );
}
