import { useState, useEffect } from "react";
import { fetchJSON } from "../api/client";
import { X, Clock, BarChart3 } from "lucide-react";

interface TopCampaign {
  campaign_id: string;
  campaign_name: string;
  impressions: number;
  clicks: number;
  conversions: number;
  ctr: number;
}

interface ChannelBreak {
  campaign_channel: string;
  clicks: number;
  impressions: number;
}

interface HourData {
  date: string;
  hour: number;
  top_campaigns: TopCampaign[];
  channel_breakdown: ChannelBreak[];
}

interface Props {
  date: string;
  hour: number;
  onClose: () => void;
}

export function HourDetail({ date, hour, onClose }: Props) {
  const [data, setData] = useState<HourData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetchJSON<HourData>(
      `/campaigns/hour-detail?date=${encodeURIComponent(date)}&hour=${hour}`
    )
      .then(setData)
      .finally(() => setLoading(false));
  }, [date, hour]);

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl w-full max-w-lg max-h-[80vh] overflow-y-auto">
        <div className="flex items-center justify-between p-5 border-b border-slate-800">
          <div className="flex items-center gap-2">
            <Clock className="w-5 h-5 text-sky-400" />
            <h3 className="text-lg font-semibold text-white">
              {date} — {String(hour).padStart(2, "0")}:00
            </h3>
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-white transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        {loading ? (
          <div className="flex items-center justify-center p-10">
            <div className="w-6 h-6 border-2 border-sky-400 border-t-transparent rounded-full animate-spin" />
          </div>
        ) : data ? (
          <div className="p-5 space-y-5">
            {/* Top Campaigns */}
            <div>
              <h4 className="text-sm font-semibold text-slate-300 mb-3 flex items-center gap-2">
                <BarChart3 className="w-4 h-4 text-emerald-400" />
                Top Campanhas neste horário
              </h4>
              {data.top_campaigns.length === 0 ? (
                <p className="text-xs text-slate-500">Nenhuma campanha neste horário.</p>
              ) : (
                <div className="space-y-2">
                  {data.top_campaigns.map((c, i) => (
                    <div
                      key={c.campaign_id}
                      className="flex items-center justify-between bg-slate-800/60 rounded-lg px-3 py-2"
                    >
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="text-xs font-bold text-slate-500 w-5">{i + 1}.</span>
                        <span className="text-sm text-white truncate">{c.campaign_name}</span>
                      </div>
                      <div className="flex items-center gap-4 text-xs shrink-0">
                        <span className="text-slate-400">
                          {c.clicks.toLocaleString("pt-BR")} clicks
                        </span>
                        <span className="text-emerald-400 font-semibold">
                          {c.ctr.toFixed(2)}%
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Channel Breakdown */}
            <div>
              <h4 className="text-sm font-semibold text-slate-300 mb-3">
                Breakdown por Canal
              </h4>
              {data.channel_breakdown.length === 0 ? (
                <p className="text-xs text-slate-500">Sem dados de canal.</p>
              ) : (
                <div className="space-y-2">
                  {data.channel_breakdown.map((ch) => {
                    const maxClicks = Math.max(...data.channel_breakdown.map((c) => c.clicks), 1);
                    const pct = (ch.clicks / maxClicks) * 100;
                    return (
                      <div key={ch.campaign_channel}>
                        <div className="flex items-center justify-between text-xs mb-1">
                          <span className="text-slate-300">{ch.campaign_channel}</span>
                          <span className="text-slate-400">
                            {ch.clicks.toLocaleString("pt-BR")} clicks
                          </span>
                        </div>
                        <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-gradient-to-r from-sky-500 to-emerald-500 rounded-full transition-all duration-500"
                            style={{ width: `${pct}%` }}
                          />
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
