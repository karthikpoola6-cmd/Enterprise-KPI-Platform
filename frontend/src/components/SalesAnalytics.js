import { useState, useEffect } from 'react';
import KPICard from './KPICard';
import TrendChart from './TrendChart';
import BarChart from './BarChart';
import DataTable from './DataTable';
import { getSalesSummary, getSalesByRegion, getSalesByProduct, getSalesBySegment, getKPITrends } from '../api';

function SalesAnalytics() {
  const [summary, setSummary] = useState(null);
  const [regions, setRegions] = useState([]);
  const [products, setProducts] = useState([]);
  const [segments, setSegments] = useState([]);
  const [orderTrend, setOrderTrend] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      getSalesSummary(),
      getSalesByRegion(),
      getSalesByProduct(),
      getSalesBySegment(),
      getKPITrends('Order Count', 'overall'),
    ]).then(([sumRes, regRes, prodRes, segRes, trendRes]) => {
      setSummary(sumRes.data);
      setRegions(regRes.data);
      setProducts(prodRes.data);
      setSegments(segRes.data);
      setOrderTrend(trendRes.data.map(d => ({
        month: d.month,
        orders: d.kpi_value,
      })));
      setLoading(false);
    }).catch(err => {
      console.error('Failed to load sales:', err);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="loading">Loading sales analytics...</div>;

  const fmt = (n) => `$${Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

  const productColumns = [
    { key: 'category', label: 'Category' },
    { key: 'revenue', label: 'Revenue', align: 'right', render: (v) => fmt(v) },
    { key: 'orders', label: 'Orders', align: 'right', render: (v) => v.toLocaleString() },
    { key: 'gross_profit', label: 'Gross Profit', align: 'right', render: (v) => fmt(v) },
    { key: 'margin_pct', label: 'Margin %', align: 'right', render: (v) => (
      <span className={v >= 0 ? 'positive' : 'negative'}>{v.toFixed(1)}%</span>
    )},
  ];

  const segmentColumns = [
    { key: 'segment', label: 'Segment' },
    { key: 'revenue', label: 'Revenue', align: 'right', render: (v) => fmt(v) },
    { key: 'orders', label: 'Orders', align: 'right', render: (v) => v.toLocaleString() },
    { key: 'avg_order_value', label: 'Avg Order Value', align: 'right', render: (v) => fmt(v) },
  ];

  return (
    <div>
      <h2>Sales Analytics</h2>
      <p className="section-subtitle">Sales performance across regions, products, and customer segments</p>

      {summary && (
        <div className="kpi-grid">
          <KPICard label="Total Revenue" value={summary.total_revenue} unit="$" className="revenue" />
          <KPICard label="Total Orders" value={summary.total_orders} unit="count" className="orders" />
          <KPICard label="Avg Order Value" value={summary.avg_order_value} unit="$" className="growth" />
          <KPICard label="Gross Margin" value={summary.gross_margin_pct} unit="%" className="margin" />
          <KPICard label="Fulfillment Rate" value={summary.fulfillment_rate} unit="%" className="fulfillment" />
        </div>
      )}

      <div className="charts-row">
        <div className="chart-card full-width">
          <h4>Monthly Order Volume</h4>
          <TrendChart data={orderTrend} dataKey="orders" color="#3b82f6" />
        </div>
      </div>

      <div className="charts-row">
        <div className="chart-card">
          <h4>Revenue by Region</h4>
          <BarChart
            data={regions.map(r => ({ name: r.region, revenue: Math.round(r.revenue) }))}
            dataKey="revenue" color="#10b981" unit="$"
          />
        </div>
        <div className="chart-card">
          <h4>Revenue by Product Category</h4>
          <BarChart
            data={products.map(p => ({ name: p.category, revenue: Math.round(p.revenue) }))}
            dataKey="revenue" color="#8b5cf6" unit="$"
          />
        </div>
      </div>

      <h3>Product Performance</h3>
      <DataTable columns={productColumns} data={products} />

      <h3>Customer Segments</h3>
      <DataTable columns={segmentColumns} data={segments} />
    </div>
  );
}

export default SalesAnalytics;
