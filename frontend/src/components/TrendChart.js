import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';

function TrendChart({ data, dataKey, xKey = 'month', color = '#3b82f6', unit = '' }) {
  const formatTooltip = (value) => {
    if (unit === '$') {
      return `$${Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
    }
    if (unit === '%') return `${value}%`;
    return value.toLocaleString();
  };

  return (
    <ResponsiveContainer width="100%" height={250}>
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis dataKey={xKey} tick={{ fontSize: 12 }} stroke="#94a3b8" />
        <YAxis tick={{ fontSize: 12 }} stroke="#94a3b8" />
        <Tooltip formatter={formatTooltip} />
        <Line type="monotone" dataKey={dataKey} stroke={color} strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}

export default TrendChart;
