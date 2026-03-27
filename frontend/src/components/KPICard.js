function KPICard({ label, value, unit, className }) {
  const formatValue = () => {
    if (unit === '$') {
      const num = Number(value);
      if (num >= 1000000) return `$${(num / 1000000).toFixed(1)}M`;
      if (num >= 1000) return `$${(num / 1000).toFixed(0)}K`;
      return `$${num.toFixed(0)}`;
    }
    if (unit === '%') return `${Number(value).toFixed(1)}%`;
    if (unit === 'count') {
      const num = Number(value);
      if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
      return num.toLocaleString();
    }
    return value;
  };

  return (
    <div className={`kpi-card ${className || ''}`}>
      <div className="kpi-value">{formatValue()}</div>
      <div className="kpi-label">{label}</div>
    </div>
  );
}

export default KPICard;
