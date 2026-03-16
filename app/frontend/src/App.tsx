import { useState, useEffect, useCallback } from "react";
import { fetchJSON } from "./api/client";
import { useAutoRefresh } from "./hooks/useAutoRefresh";
import { KPICards } from "./components/KPICards";
import { ClickTrendChart, Granularity } from "./components/ClickTrendChart";
import { TopCampaignsTable } from "./components/TopCampaignsTable";
import { ChannelPerformance } from "./components/ChannelPerformance";
import { SegmentBreakdown } from "./components/SegmentBreakdown";
import { ABTestResults } from "./components/ABTestResults";
import { GeoPerformance } from "./components/GeoPerformance";
import { CampaignSearch } from "./components/CampaignSearch";
import { HourDetail } from "./components/HourDetail";
import { CampaignComparator } from "./components/CampaignComparator";
import { Activity, X, Search, ArrowLeftRight } from "lucide-react";

interface KPISummary {
  total_impressions: number;
  total_clicks: number;
  ctr: number;
  total_conversions: number;
  total_events: number;
  avg_response_time_ms: number;
}

interface Filters {
  campaignId: string | null;
  campaignName: string | null;
  channel: string | null;
  segment: string | null;
}

export default function App() {
  const refreshKey = useAutoRefresh(1000);
  const [kpis, setKpis] = useState<KPISummary | null>(null);
  const [hourly, setHourly] = useState<any[]>([]);
  const [campaigns, setCampaigns] = useState<any[]>([]);
  const [channels, setChannels] = useState<any[]>([]);
  const [segments, setSegments] = useState<any[]>([]);
  const [abTest, setAbTest] = useState<any[]>([]);
  const [geo, setGeo] = useState<any[]>([]);
  const [lastRefresh, setLastRefresh] = useState(new Date());
  const [expandedKpi, setExpandedKpi] = useState<string | null>(null);
  const [filters, setFilters] = useState<Filters>({
    campaignId: null, campaignName: null, channel: null, segment: null,
  });

  const [granularity, setGranularity] = useState<Granularity>("hour");
  const [hourDetail, setHourDetail] = useState<{ date: string; hour: number } | null>(null);
  const [showComparator, setShowComparator] = useState(false);

  const hasFilters = filters.campaignId || filters.channel || filters.segment;

  const clearFilters = useCallback(() => {
    setFilters({ campaignId: null, campaignName: null, channel: null, segment: null });
  }, []);

  useEffect(() => {
    const trendParams = new URLSearchParams({ granularity });
    if (filters.campaignId) trendParams.set("campaign_id", filters.campaignId);
    const trendUrl = `/campaigns/trend?${trendParams}`;

    Promise.all([
      fetchJSON<KPISummary>("/kpis/summary").then(setKpis),
      fetchJSON<any[]>(trendUrl).then(setHourly),
      fetchJSON<any[]>("/campaigns/performance").then(setCampaigns),
      fetchJSON<any[]>("/channels").then(setChannels),
      fetchJSON<any[]>("/segments").then(setSegments),
      fetchJSON<any[]>("/ab-test").then(setAbTest),
      fetchJSON<any[]>("/geo").then(setGeo),
    ]).then(() => setLastRefresh(new Date()));
  }, [refreshKey, filters.campaignId, granularity]);

  // Apply cross-filters to campaigns table
  const filteredCampaigns = campaigns.filter((c) => {
    if (filters.channel && c.campaign_channel !== filters.channel) return false;
    if (filters.segment && c.target_segment !== filters.segment) return false;
    return true;
  });

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold bg-gradient-to-r from-sky-400 via-emerald-400 to-purple-400 bg-clip-text text-transparent">CRM Campaign Monitor</h1>
          <p className="text-slate-400 text-sm mt-1">
            Monitoramento real-time de campanhas CRM - Mobile Banking
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 bg-emerald-500/10 border border-emerald-500/30 rounded-full px-3 py-1.5">
            <span className="relative flex h-2.5 w-2.5">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500"></span>
            </span>
            <span className="text-emerald-400 text-xs font-semibold tracking-wide">LIVE</span>
          </div>
          <div className="flex items-center gap-2 text-slate-400 text-xs">
            <Activity className="w-3.5 h-3.5" />
            <span>{lastRefresh.toLocaleTimeString("pt-BR")}</span>
          </div>
        </div>
      </div>

      {/* Search + Compare */}
      <div className="flex items-center gap-3 mb-4 flex-wrap">
        <CampaignSearch
          onSelectCampaign={(id, name) =>
            setFilters((f) => ({ ...f, campaignId: id, campaignName: name }))
          }
        />
        <button
          onClick={() => setShowComparator(true)}
          className="flex items-center gap-2 bg-purple-500/10 border border-purple-500/30 text-purple-300 rounded-lg px-4 py-2 text-sm font-medium hover:bg-purple-500/20 transition-all shrink-0"
        >
          <ArrowLeftRight className="w-4 h-4" />
          Comparar
        </button>
      </div>

      {/* Active Filters Bar */}
      {hasFilters && (
        <div className="flex items-center gap-2 mb-4 flex-wrap">
          <span className="text-slate-400 text-xs font-medium">Filtros:</span>
          {filters.campaignName && (
            <FilterBadge
              label={`Campanha: ${filters.campaignName}`}
              color="bg-sky-500/20 text-sky-300 border-sky-500/30"
              onRemove={() => setFilters((f) => ({ ...f, campaignId: null, campaignName: null }))}
            />
          )}
          {filters.channel && (
            <FilterBadge
              label={`Canal: ${filters.channel}`}
              color="bg-emerald-500/20 text-emerald-300 border-emerald-500/30"
              onRemove={() => setFilters((f) => ({ ...f, channel: null }))}
            />
          )}
          {filters.segment && (
            <FilterBadge
              label={`Segmento: ${filters.segment}`}
              color="bg-amber-500/20 text-amber-300 border-amber-500/30"
              onRemove={() => setFilters((f) => ({ ...f, segment: null }))}
            />
          )}
          <button
            onClick={clearFilters}
            className="text-xs text-slate-500 hover:text-slate-300 underline ml-2 transition-colors"
          >
            Limpar todos
          </button>
        </div>
      )}

      {/* KPI Cards */}
      {kpis && (
        <KPICards
          data={kpis}
          expandedKpi={expandedKpi}
          onToggleKpi={(key) => setExpandedKpi(expandedKpi === key ? null : key)}
        />
      )}

      {/* Charts Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
        <div className="lg:col-span-2">
          <ClickTrendChart
            data={hourly}
            campaignName={filters.campaignName}
            granularity={granularity}
            onGranularityChange={setGranularity}
            onClickPoint={(date, hour) => setHourDetail({ date, hour })}
          />
        </div>
        <TopCampaignsTable
          data={filteredCampaigns}
          selectedCampaignId={filters.campaignId}
          onSelectCampaign={(id, name) =>
            setFilters((f) => ({
              ...f,
              campaignId: f.campaignId === id ? null : id,
              campaignName: f.campaignId === id ? null : name,
            }))
          }
        />
        <ChannelPerformance
          data={channels}
          selectedChannel={filters.channel}
          onSelectChannel={(ch) =>
            setFilters((f) => ({ ...f, channel: f.channel === ch ? null : ch }))
          }
        />
        <SegmentBreakdown
          data={segments}
          selectedSegment={filters.segment}
          onSelectSegment={(seg) =>
            setFilters((f) => ({ ...f, segment: f.segment === seg ? null : seg }))
          }
        />
        <ABTestResults data={abTest} />
        <div className="lg:col-span-2">
          <GeoPerformance data={geo} />
        </div>
      </div>

      {/* Modals */}
      {hourDetail && (
        <HourDetail
          date={hourDetail.date}
          hour={hourDetail.hour}
          onClose={() => setHourDetail(null)}
        />
      )}
      {showComparator && (
        <CampaignComparator onClose={() => setShowComparator(false)} />
      )}
    </div>
  );
}

function FilterBadge({ label, color, onRemove }: { label: string; color: string; onRemove: () => void }) {
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${color}`}>
      {label}
      <button onClick={onRemove} className="hover:opacity-70 transition-opacity">
        <X className="w-3 h-3" />
      </button>
    </span>
  );
}
