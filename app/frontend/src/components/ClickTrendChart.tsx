import { useState } from "react";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";

interface TrendData {
  event_date: string;
  hour_of_day: number;
  minute_of_hour?: number;
  impressions: number;
  clicks: number;
  conversions?: number;
}

export type Granularity = "hour" | "minute";

interface Props {
  data: TrendData[];
  campaignName?: string | null;
  granularity: Granularity;
  onGranularityChange: (g: Granularity) => void;
  onClickPoint?: (date: string, hour: number) => void;
}

const METRICS = [
  { key: "impressions", label: "Impressions", color: "#38bdf8", gradient: "colorImpressions" },
  { key: "clicks", label: "Clicks", color: "#34d399", gradient: "colorClicks" },
  { key: "conversions", label: "Conversions", color: "#a78bfa", gradient: "colorConversions" },
];

const GRANULARITY_OPTIONS: { key: Granularity; label: string }[] = [
  { key: "hour", label: "Hora (48h)" },
  { key: "minute", label: "Minuto (1h)" },
];

export function ClickTrendChart({ data, campaignName, granularity, onGranularityChange, onClickPoint }: Props) {
  const [visible, setVisible] = useState<Record<string, boolean>>({
    impressions: true,
    clicks: true,
    conversions: false,
  });

  const isMinute = granularity === "minute";

  const sorted = [...data].sort((a, b) => {
    const cmp = a.event_date.localeCompare(b.event_date);
    if (cmp !== 0) return cmp;
    const hCmp = a.hour_of_day - b.hour_of_day;
    if (hCmp !== 0) return hCmp;
    return (a.minute_of_hour ?? 0) - (b.minute_of_hour ?? 0);
  });

  const sliceCount = isMinute ? 60 : 48;
  const formatted = sorted.slice(-sliceCount).map((d) => {
    const hh = String(d.hour_of_day).padStart(2, "0");
    const mm = String(d.minute_of_hour ?? 0).padStart(2, "0");
    return {
      ...d,
      label: isMinute ? `${hh}:${mm}` : `${hh}:00`,
      fullLabel: isMinute
        ? `${d.event_date} ${hh}:${mm}`
        : `${d.event_date} ${hh}:00`,
    };
  });

  const toggleMetric = (key: string) => {
    setVisible((v) => ({ ...v, [key]: !v[key] }));
  };

  const title = campaignName
    ? `Tendência: ${campaignName}`
    : isMinute
      ? "Tendência de Clicks e Impressions (última hora)"
      : "Tendência de Clicks e Impressions (últimas 48h)";

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
        <h3 className="text-lg font-semibold text-white">{title}</h3>
        <div className="flex items-center gap-3">
          {/* Granularity selector */}
          <div className="flex items-center bg-slate-800 rounded-lg p-0.5">
            {GRANULARITY_OPTIONS.map(({ key, label }) => (
              <button
                key={key}
                onClick={() => onGranularityChange(key)}
                className={`px-3 py-1 rounded-md text-xs font-medium transition-all duration-200 ${
                  granularity === key
                    ? "bg-sky-500/20 text-sky-300 border border-sky-500/40"
                    : "text-slate-400 hover:text-slate-300 border border-transparent"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
          {/* Metric toggles */}
          <div className="flex items-center gap-1">
            {METRICS.map(({ key, label, color }) => (
              <button
                key={key}
                onClick={() => toggleMetric(key)}
                className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-all duration-200 ${
                  visible[key]
                    ? "border-opacity-50 text-white"
                    : "border-slate-700 text-slate-500 opacity-50"
                }`}
                style={{
                  borderColor: visible[key] ? color : undefined,
                  backgroundColor: visible[key] ? `${color}20` : "transparent",
                }}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <AreaChart
          data={formatted}
          onClick={(e) => {
            if (e?.activePayload?.[0]?.payload && onClickPoint) {
              const pt = e.activePayload[0].payload;
              onClickPoint(pt.event_date, pt.hour_of_day);
            }
          }}
          style={{ cursor: onClickPoint ? "pointer" : undefined }}
        >
          <defs>
            <linearGradient id="colorImpressions" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#38bdf8" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#34d399" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#34d399" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="colorConversions" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#a78bfa" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#a78bfa" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="label" tick={{ fill: "#94a3b8", fontSize: 11 }} interval="preserveStartEnd" />
          <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <Tooltip
            contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
            labelStyle={{ color: "#f1f5f9" }}
            labelFormatter={(_val, payload) => payload?.[0]?.payload?.fullLabel ?? _val}
          />
          <Legend />
          {METRICS.map(({ key, color, gradient, label }) =>
            visible[key] ? (
              <Area
                key={key}
                type="monotone"
                dataKey={key}
                stroke={color}
                fill={`url(#${gradient})`}
                name={label}
                isAnimationActive
                animationDuration={600}
                animationEasing="ease-in-out"
              />
            ) : null
          )}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
