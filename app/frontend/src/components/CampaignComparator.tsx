import { useState, useEffect } from "react";
import { fetchJSON } from "../api/client";
import { CampaignSearch } from "./CampaignSearch";
import { ArrowLeftRight, X, TrendingUp, TrendingDown, Minus } from "lucide-react";
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";

interface CampaignPerf {
  campaign_id: string;
  campaign_name: string;
  campaign_category: string;
  campaign_channel: string;
  total_impressions: number;
  total_clicks: number;
  total_conversions: number;
  ctr: number;
  conversion_rate: number;
  total_conversion_value: number;
  cost_per_click: number;
  total_events: number;
}

interface HourlyPoint {
  event_date: string;
  hour_of_day: number;
  impressions: number;
  clicks: number;
  conversions: number;
  ctr: number;
}

interface CompareData {
  campaign1: CampaignPerf | null;
  campaign2: CampaignPerf | null;
  hourly1: HourlyPoint[];
  hourly2: HourlyPoint[];
}

interface Props {
  onClose: () => void;
}

function MetricRow({ label, v1, v2, format }: { label: string; v1: number; v2: number; format: (n: number) => string }) {
  const diff = v1 - v2;
  return (
    <div className="grid grid-cols-3 gap-2 py-2 border-b border-slate-800/50 last:border-0">
      <div className="text-xs text-slate-400">{label}</div>
      <div className="text-sm text-white text-center font-medium">{format(v1)}</div>
      <div className="text-sm text-white text-center font-medium flex items-center justify-center gap-1">
        {format(v2)}
        {diff > 0 ? (
          <TrendingUp className="w-3 h-3 text-emerald-400" />
        ) : diff < 0 ? (
          <TrendingDown className="w-3 h-3 text-red-400" />
        ) : (
          <Minus className="w-3 h-3 text-slate-500" />
        )}
      </div>
    </div>
  );
}

export function CampaignComparator({ onClose }: Props) {
  const [sel1, setSel1] = useState<{ id: string; name: string } | null>(null);
  const [sel2, setSel2] = useState<{ id: string; name: string } | null>(null);
  const [data, setData] = useState<CompareData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!sel1 || !sel2) { setData(null); return; }
    setLoading(true);
    fetchJSON<CompareData>(
      `/campaigns/compare?id1=${encodeURIComponent(sel1.id)}&id2=${encodeURIComponent(sel2.id)}`
    )
      .then(setData)
      .finally(() => setLoading(false));
  }, [sel1, sel2]);

  // Merge hourly data for chart
  const chartData = data
    ? (() => {
        const map = new Map<string, any>();
        for (const h of data.hourly1) {
          const key = `${h.event_date} ${String(h.hour_of_day).padStart(2, "0")}:00`;
          map.set(key, { label: key, clicks1: h.clicks, clicks2: 0 });
        }
        for (const h of data.hourly2) {
          const key = `${h.event_date} ${String(h.hour_of_day).padStart(2, "0")}:00`;
          const existing = map.get(key) ?? { label: key, clicks1: 0, clicks2: 0 };
          existing.clicks2 = h.clicks;
          map.set(key, existing);
        }
        return Array.from(map.values()).sort((a, b) => a.label.localeCompare(b.label));
      })()
    : [];

  const fmt = (n: number) => n.toLocaleString("pt-BR");
  const fmtPct = (n: number) => `${n.toFixed(2)}%`;
  const fmtBrl = (n: number) => `R$ ${n.toLocaleString("pt-BR", { minimumFractionDigits: 2 })}`;

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl w-full max-w-3xl max-h-[85vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-slate-800 sticky top-0 bg-slate-900 z-10">
          <div className="flex items-center gap-2">
            <ArrowLeftRight className="w-5 h-5 text-purple-400" />
            <h3 className="text-lg font-semibold text-white">Comparar Campanhas</h3>
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-white transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-5 space-y-5">
          {/* Search pickers */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="text-xs text-slate-400 mb-1.5 block">Campanha 1</label>
              {sel1 ? (
                <div className="flex items-center gap-2 bg-sky-500/10 border border-sky-500/30 rounded-lg px-3 py-2">
                  <span className="text-sm text-sky-300 truncate flex-1">{sel1.name}</span>
                  <button onClick={() => setSel1(null)} className="text-sky-400 hover:text-white">
                    <X className="w-4 h-4" />
                  </button>
                </div>
              ) : (
                <CampaignSearch onSelectCampaign={(id, name) => setSel1({ id, name })} />
              )}
            </div>
            <div>
              <label className="text-xs text-slate-400 mb-1.5 block">Campanha 2</label>
              {sel2 ? (
                <div className="flex items-center gap-2 bg-purple-500/10 border border-purple-500/30 rounded-lg px-3 py-2">
                  <span className="text-sm text-purple-300 truncate flex-1">{sel2.name}</span>
                  <button onClick={() => setSel2(null)} className="text-purple-400 hover:text-white">
                    <X className="w-4 h-4" />
                  </button>
                </div>
              ) : (
                <CampaignSearch onSelectCampaign={(id, name) => setSel2({ id, name })} />
              )}
            </div>
          </div>

          {loading && (
            <div className="flex items-center justify-center py-8">
              <div className="w-6 h-6 border-2 border-purple-400 border-t-transparent rounded-full animate-spin" />
            </div>
          )}

          {data && data.campaign1 && data.campaign2 && (
            <>
              {/* Metrics comparison */}
              <div className="bg-slate-800/40 rounded-xl p-4">
                <div className="grid grid-cols-3 gap-2 pb-2 border-b border-slate-700 mb-1">
                  <div className="text-xs text-slate-500 font-medium">Métrica</div>
                  <div className="text-xs text-sky-400 text-center font-medium truncate">{data.campaign1.campaign_name}</div>
                  <div className="text-xs text-purple-400 text-center font-medium truncate">{data.campaign2.campaign_name}</div>
                </div>
                <MetricRow label="Impressions" v1={data.campaign1.total_impressions} v2={data.campaign2.total_impressions} format={fmt} />
                <MetricRow label="Clicks" v1={data.campaign1.total_clicks} v2={data.campaign2.total_clicks} format={fmt} />
                <MetricRow label="CTR" v1={data.campaign1.ctr} v2={data.campaign2.ctr} format={fmtPct} />
                <MetricRow label="Conversões" v1={data.campaign1.total_conversions} v2={data.campaign2.total_conversions} format={fmt} />
                <MetricRow label="Taxa Conv." v1={data.campaign1.conversion_rate} v2={data.campaign2.conversion_rate} format={fmtPct} />
                <MetricRow label="Valor Conv." v1={data.campaign1.total_conversion_value} v2={data.campaign2.total_conversion_value} format={fmtBrl} />
                <MetricRow label="CPC" v1={data.campaign1.cost_per_click} v2={data.campaign2.cost_per_click} format={fmtBrl} />
              </div>

              {/* Hourly chart comparison */}
              {chartData.length > 0 && (
                <div>
                  <h4 className="text-sm font-semibold text-slate-300 mb-3">Tendência de Clicks (últimas 48h)</h4>
                  <ResponsiveContainer width="100%" height={220}>
                    <AreaChart data={chartData}>
                      <defs>
                        <linearGradient id="cmpGrad1" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.3} />
                          <stop offset="95%" stopColor="#38bdf8" stopOpacity={0} />
                        </linearGradient>
                        <linearGradient id="cmpGrad2" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#a78bfa" stopOpacity={0.3} />
                          <stop offset="95%" stopColor="#a78bfa" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="label" tick={{ fill: "#94a3b8", fontSize: 10 }} interval="preserveStartEnd" />
                      <YAxis tick={{ fill: "#94a3b8", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }} />
                      <Legend />
                      <Area type="monotone" dataKey="clicks1" name={data.campaign1.campaign_name} stroke="#38bdf8" fill="url(#cmpGrad1)" />
                      <Area type="monotone" dataKey="clicks2" name={data.campaign2.campaign_name} stroke="#a78bfa" fill="url(#cmpGrad2)" />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              )}
            </>
          )}

          {!sel1 || !sel2 ? (
            <p className="text-center text-sm text-slate-500 py-4">
              Selecione duas campanhas para comparar
            </p>
          ) : null}
        </div>
      </div>
    </div>
  );
}
