import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from "recharts";

interface Channel {
  campaign_channel: string;
  total_clicks: number;
  total_impressions: number;
  ctr: number;
}

interface Props {
  data: Channel[];
  selectedChannel: string | null;
  onSelectChannel: (channel: string) => void;
}

export function ChannelPerformance({ data, selectedChannel, onSelectChannel }: Props) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h3 className="text-lg font-semibold text-white mb-1">Performance por Canal</h3>
      {selectedChannel && (
        <p className="text-xs text-emerald-400 mb-3 animate-pulse">
          Filtro ativo: {selectedChannel} (clique novamente para remover)
        </p>
      )}
      {!selectedChannel && <div className="mb-4" />}
      <ResponsiveContainer width="100%" height={250}>
        <BarChart
          data={data}
          layout="vertical"
          onClick={(e) => {
            if (e?.activeLabel) onSelectChannel(e.activeLabel);
          }}
          style={{ cursor: "pointer" }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis type="number" tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <YAxis type="category" dataKey="campaign_channel" tick={{ fill: "#94a3b8", fontSize: 11 }} width={100} />
          <Tooltip
            contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
            labelStyle={{ color: "#f1f5f9" }}
          />
          <Bar dataKey="total_clicks" name="Clicks" radius={[0, 4, 4, 0]}>
            {data.map((d) => (
              <Cell
                key={d.campaign_channel}
                fill={!selectedChannel || selectedChannel === d.campaign_channel ? "#34d399" : "#334155"}
                style={{ transition: "fill 0.3s ease" }}
              />
            ))}
          </Bar>
          <Bar dataKey="total_impressions" name="Impressions" radius={[0, 4, 4, 0]}>
            {data.map((d) => (
              <Cell
                key={d.campaign_channel}
                fill={!selectedChannel || selectedChannel === d.campaign_channel ? "#38bdf8" : "#1e293b"}
                style={{ transition: "fill 0.3s ease" }}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
