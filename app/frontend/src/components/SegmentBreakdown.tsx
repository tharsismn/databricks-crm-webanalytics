import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend, Sector } from "recharts";
import { useCallback } from "react";

interface Segment {
  target_segment: string;
  total_clicks: number;
}

interface Props {
  data: Segment[];
  selectedSegment: string | null;
  onSelectSegment: (segment: string) => void;
}

const COLORS = ["#38bdf8", "#34d399", "#f59e0b", "#a78bfa", "#f87171"];
const DIM_COLORS = ["#1e3a5f", "#1a3d2e", "#3d2e0a", "#2d2548", "#3d1a1a"];

const renderActiveShape = (props: any) => {
  const { cx, cy, innerRadius, outerRadius, startAngle, endAngle, fill, payload, percent } = props;
  return (
    <g>
      <Sector
        cx={cx} cy={cy} innerRadius={innerRadius} outerRadius={outerRadius + 8}
        startAngle={startAngle} endAngle={endAngle} fill={fill}
        style={{ filter: "drop-shadow(0 0 6px rgba(255,255,255,0.2))", transition: "all 0.3s ease" }}
      />
      <text x={cx} y={cy - 8} textAnchor="middle" fill="#f1f5f9" fontSize={14} fontWeight="bold">
        {payload.target_segment}
      </text>
      <text x={cx} y={cy + 12} textAnchor="middle" fill="#94a3b8" fontSize={12}>
        {(percent * 100).toFixed(1)}%
      </text>
    </g>
  );
};

export function SegmentBreakdown({ data, selectedSegment, onSelectSegment }: Props) {
  const activeIndex = selectedSegment ? data.findIndex((d) => d.target_segment === selectedSegment) : -1;

  const handleClick = useCallback((_: any, index: number) => {
    if (data[index]) onSelectSegment(data[index].target_segment);
  }, [data, onSelectSegment]);

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h3 className="text-lg font-semibold text-white mb-1">Distribuição por Segmento</h3>
      {selectedSegment && (
        <p className="text-xs text-amber-400 mb-3 animate-pulse">
          Filtro ativo: {selectedSegment} (clique novamente para remover)
        </p>
      )}
      {!selectedSegment && <div className="mb-4" />}
      <ResponsiveContainer width="100%" height={280}>
        <PieChart>
          <Pie
            data={data}
            dataKey="total_clicks"
            nameKey="target_segment"
            cx="50%"
            cy="55%"
            outerRadius={75}
            activeIndex={activeIndex >= 0 ? activeIndex : undefined}
            activeShape={activeIndex >= 0 ? renderActiveShape : undefined}
            onClick={handleClick}
            style={{ cursor: "pointer" }}
            label={activeIndex < 0 ? ({ target_segment, percent }) =>
              `${target_segment} ${(percent * 100).toFixed(0)}%` : undefined}
            labelLine={activeIndex < 0 ? { stroke: "#64748b" } : false}
          >
            {data.map((d, i) => (
              <Cell
                key={i}
                fill={!selectedSegment || selectedSegment === d.target_segment
                  ? COLORS[i % COLORS.length]
                  : DIM_COLORS[i % DIM_COLORS.length]}
                style={{ transition: "fill 0.3s ease", cursor: "pointer" }}
              />
            ))}
          </Pie>
          <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }} />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}
