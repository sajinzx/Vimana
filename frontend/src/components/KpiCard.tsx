interface Props {
  label: string;
  value: string | number | null | undefined;
}

export default function KpiCard({ label, value }: Props) {
  return (
    <div className="kpi">
      <b>{value ?? "–"}</b>
      <span>{label}</span>
    </div>
  );
}
