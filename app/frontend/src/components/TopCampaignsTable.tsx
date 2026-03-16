interface Campaign {
  campaign_id: string;
  campaign_name: string;
  campaign_channel: string;
  total_impressions: number;
  total_clicks: number;
  total_conversions: number;
  ctr: number;
  conversion_rate: number;
}

interface Props {
  data: Campaign[];
  selectedCampaignId: string | null;
  onSelectCampaign: (id: string, name: string) => void;
}

export function TopCampaignsTable({ data, selectedCampaignId, onSelectCampaign }: Props) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h3 className="text-lg font-semibold text-white mb-4">Top Campanhas por CTR</h3>
      <div className="overflow-x-auto max-h-[300px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-slate-900">
            <tr className="text-slate-400 border-b border-slate-800">
              <th className="text-left py-2 font-medium">Campanha</th>
              <th className="text-right py-2 font-medium">Canal</th>
              <th className="text-right py-2 font-medium">Clicks</th>
              <th className="text-right py-2 font-medium">CTR</th>
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 12).map((c) => {
              const isSelected = selectedCampaignId === c.campaign_id;
              return (
                <tr
                  key={c.campaign_id}
                  onClick={() => onSelectCampaign(c.campaign_id, c.campaign_name)}
                  className={`border-b border-slate-800/50 cursor-pointer transition-all duration-200
                    ${isSelected
                      ? "bg-sky-500/15 border-l-2 border-l-sky-400"
                      : "hover:bg-slate-800/40"
                    }`}
                >
                  <td className="py-2.5 text-white font-medium truncate max-w-[200px]">
                    {c.campaign_name}
                  </td>
                  <td className="py-2.5 text-right">
                    <span className="px-2 py-0.5 bg-slate-800 rounded text-xs text-slate-300">
                      {c.campaign_channel}
                    </span>
                  </td>
                  <td className="py-2.5 text-right text-slate-300">
                    {c.total_clicks.toLocaleString("pt-BR")}
                  </td>
                  <td className="py-2.5 text-right font-semibold text-emerald-400">
                    {c.ctr.toFixed(2)}%
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {selectedCampaignId && (
        <p className="text-xs text-sky-400 mt-2 text-center animate-pulse">
          Gráfico de tendência filtrado por campanha
        </p>
      )}
    </div>
  );
}
