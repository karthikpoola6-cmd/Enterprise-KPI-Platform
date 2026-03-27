import { ResponsiveContainer, BarChart as RechartsBar, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';

function BarChart({ data, dataKey, xKey = 'name', color = '#3b82f6', unit = '' }) {
  const formatTooltip = (value) => {
    if (unit === '$') {
      return `$${Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
    }
    if (unit === '%') return `${value}%`;
    return value.toLocaleString();
  };

  return (
    <ResponsiveContainer width="100%" height={250}>
      <RechartsBar data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis dataKey={xKey} tick={{ fontSize: 12 }} stroke="#94a3b8" />
        <YAxis tick={{ fontSize: 12 }} stroke="#94a3b8" />
        <Tooltip formatter={formatTooltip} />
        <Bar dataKey={dataKey} fill={color} radius={[4, 4, 0, 0]} />
      </RechartsBar>
    </ResponsiveContainer>
  );
}

export default BarChart;
