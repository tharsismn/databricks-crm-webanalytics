import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

interface GeoData {
  geo_region: string;
  geo_city: string;
  total_clicks: number;
  total_impressions: number;
  ctr: number;
}

export function GeoPerformance({ data }: { data: GeoData[] }) {
  const formatted = data.slice(0, 15).map((d) => ({
    ...d,
    label: `${d.geo_city} (${d.geo_region})`,
  }));

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h3 className="text-lg font-semibold text-white mb-4">Top Regiões por Clicks</h3>
      <ResponsiveContainer width="100%" height={350}>
        <BarChart data={formatted} layout="vertical">
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis type="number" tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <YAxis type="category" dataKey="label" tick={{ fill: "#94a3b8", fontSize: 11 }} width={180} />
          <Tooltip
            contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
            labelStyle={{ color: "#f1f5f9" }}
          />
          <Bar dataKey="total_clicks" fill="#38bdf8" name="Clicks" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
