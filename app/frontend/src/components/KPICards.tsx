import { useEffect, useRef, useState } from "react";
import { Eye, MousePointerClick, Target, ShoppingCart, TrendingUp, Zap, Clock, BarChart3 } from "lucide-react";
import { useAnimatedValue } from "../hooks/useAnimatedValue";

interface KPIData {
  total_impressions: number;
  total_clicks: number;
  ctr: number;
  total_conversions: number;
  total_events?: number;
  avg_response_time_ms?: number;
}

interface Props {
  data: KPIData;
  expandedKpi: string | null;
  onToggleKpi: (key: string) => void;
}

const cardDefs = [
  {
    key: "total_impressions", label: "Impressions",
    icon: Eye, color: "text-sky-400", bg: "bg-sky-400/10", ring: "ring-sky-400/40",
    detailIcon: BarChart3, detailLabel: "Total de vezes que campanhas foram exibidas",
  },
  {
    key: "total_clicks", label: "Clicks",
    icon: MousePointerClick, color: "text-emerald-400", bg: "bg-emerald-400/10", ring: "ring-emerald-400/40",
    detailIcon: TrendingUp, detailLabel: "Total de interações com campanhas",
  },
  {
    key: "ctr", label: "CTR (%)",
    icon: Target, color: "text-amber-400", bg: "bg-amber-400/10", ring: "ring-amber-400/40", suffix: "%",
    detailIcon: Zap, detailLabel: "Taxa de conversão de impressão para click",
  },
  {
    key: "total_conversions", label: "Conversões",
    icon: ShoppingCart, color: "text-purple-400", bg: "bg-purple-400/10", ring: "ring-purple-400/40",
    detailIcon: Clock, detailLabel: "Clientes que completaram a ação da campanha",
  },
];

function AnimatedKPI({ value, isCtr, suffix, ring }: { value: number; isCtr: boolean; suffix?: string; ring: string }) {
  const animated = useAnimatedValue(value);
  const prevValue = useRef(value);
  const [flash, setFlash] = useState(false);

  useEffect(() => {
    if (prevValue.current !== value && prevValue.current !== 0) {
      setFlash(true);
      const t = setTimeout(() => setFlash(false), 1200);
      prevValue.current = value;
      return () => clearTimeout(t);
    }
    prevValue.current = value;
  }, [value]);

  return (
    <p className={`text-2xl font-bold text-white transition-all duration-500 ${flash ? `ring-2 ${ring} rounded-lg px-1 -mx-1` : ""}`}>
      {isCtr
        ? animated.toFixed(2)
        : Math.round(animated).toLocaleString("pt-BR")}
      {suffix && <span className="text-lg text-slate-400 ml-1">{suffix}</span>}
      {flash && (
        <span className="ml-2 inline-block text-xs font-normal text-emerald-400 animate-pulse">
          LIVE
        </span>
      )}
    </p>
  );
}

export function KPICards({ data, expandedKpi, onToggleKpi }: Props) {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      {cardDefs.map(({ key, label, icon: Icon, color, bg, ring, suffix, detailIcon: DetailIcon, detailLabel }) => {
        const isExpanded = expandedKpi === key;
        return (
          <div
            key={key}
            onClick={() => onToggleKpi(key)}
            className={`bg-slate-900 border rounded-xl p-5 transition-all duration-300 cursor-pointer
              ${isExpanded ? "border-slate-600 ring-1 ring-slate-600" : "border-slate-800 hover:border-slate-700"}`}
          >
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <span className="text-slate-400 text-base font-semibold block mb-3">{label}</span>
                <AnimatedKPI
                  value={data[key as keyof KPIData] as number}
                  isCtr={key === "ctr"}
                  suffix={suffix}
                  ring={ring}
                />
              </div>
              <div className={`${bg} p-3 rounded-xl flex items-center justify-center self-center`}>
                <Icon className={`w-8 h-8 ${color}`} />
              </div>
            </div>
            {isExpanded && (
              <div className="mt-3 pt-3 border-t border-slate-800 animate-in fade-in duration-300">
                <div className="flex items-center gap-2 text-slate-400 text-xs">
                  <DetailIcon className="w-3.5 h-3.5" />
                  <span>{detailLabel}</span>
                </div>
                {key === "total_clicks" && data.total_events != null && (
                  <p className="text-xs text-slate-500 mt-1.5">
                    Total de eventos: <span className="text-slate-300">{data.total_events.toLocaleString("pt-BR")}</span>
                  </p>
                )}
                {key === "ctr" && (
                  <p className="text-xs text-slate-500 mt-1.5">
                    {data.total_clicks.toLocaleString("pt-BR")} clicks / {data.total_impressions.toLocaleString("pt-BR")} impressions
                  </p>
                )}
                {key === "total_conversions" && data.total_clicks > 0 && (
                  <p className="text-xs text-slate-500 mt-1.5">
                    Taxa: <span className="text-slate-300">{((data.total_conversions / data.total_clicks) * 100).toFixed(2)}%</span> dos clicks
                  </p>
                )}
                {key === "total_impressions" && data.avg_response_time_ms != null && (
                  <p className="text-xs text-slate-500 mt-1.5">
                    Tempo médio resposta: <span className="text-slate-300">{Math.round(data.avg_response_time_ms)}ms</span>
                  </p>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
