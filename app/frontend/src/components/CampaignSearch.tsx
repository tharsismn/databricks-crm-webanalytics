import { useState, useEffect, useRef } from "react";
import { Search, X } from "lucide-react";
import { fetchJSON } from "../api/client";

interface SearchResult {
  campaign_id: string;
  campaign_name: string;
  campaign_category: string;
  campaign_channel: string;
  total_clicks: number;
  ctr: number;
}

interface Props {
  onSelectCampaign: (id: string, name: string) => void;
}

export function CampaignSearch({ onSelectCampaign }: Props) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (!query.trim()) {
      setResults([]);
      setIsOpen(false);
      return;
    }
    setLoading(true);
    debounceRef.current = setTimeout(() => {
      fetchJSON<SearchResult[]>(`/campaigns/search?q=${encodeURIComponent(query)}`)
        .then((data) => {
          setResults(data);
          setIsOpen(data.length > 0);
          setLoading(false);
        })
        .catch(() => setLoading(false));
    }, 200);
  }, [query]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return (
    <div ref={containerRef} className="relative w-full max-w-md">
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => results.length > 0 && setIsOpen(true)}
          placeholder="Buscar campanhas..."
          className="w-full bg-slate-900 border border-slate-700 rounded-lg pl-10 pr-8 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500 transition-all"
        />
        {query && (
          <button
            onClick={() => { setQuery(""); setResults([]); setIsOpen(false); }}
            className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300"
          >
            <X className="w-4 h-4" />
          </button>
        )}
        {loading && (
          <div className="absolute right-8 top-1/2 -translate-y-1/2">
            <div className="w-3 h-3 border-2 border-sky-400 border-t-transparent rounded-full animate-spin" />
          </div>
        )}
      </div>

      {isOpen && (
        <div className="absolute top-full mt-1 left-0 right-0 bg-slate-900 border border-slate-700 rounded-lg shadow-xl z-50 max-h-64 overflow-y-auto">
          {results.map((r) => (
            <button
              key={r.campaign_id}
              onClick={() => {
                onSelectCampaign(r.campaign_id, r.campaign_name);
                setQuery("");
                setIsOpen(false);
              }}
              className="w-full text-left px-4 py-2.5 hover:bg-slate-800 transition-colors border-b border-slate-800/50 last:border-0"
            >
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-white font-medium">{r.campaign_name}</p>
                  <p className="text-xs text-slate-400 mt-0.5">
                    {r.campaign_channel} | {r.campaign_category}
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-sm text-emerald-400 font-semibold">{r.ctr.toFixed(2)}%</p>
                  <p className="text-xs text-slate-500">{r.total_clicks.toLocaleString("pt-BR")} clicks</p>
                </div>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
