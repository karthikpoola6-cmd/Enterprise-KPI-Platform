import { useState, useEffect } from 'react';
import KPICard from './KPICard';
import TrendChart from './TrendChart';
import BarChart from './BarChart';
import { getKPIs, getKPITrends, getSalesByRegion, getSalesByProduct } from '../api';

function ExecutiveDashboard() {
  const [kpis, setKPIs] = useState([]);
  const [revenueTrend, setRevenueTrend] = useState([]);
  const [regionData, setRegionData] = useState([]);
  const [productData, setProductData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      getKPIs(),
      getKPITrends('Total Revenue', 'overall'),
      getSalesByRegion(),
      getSalesByProduct(),
    ]).then(([kpiRes, trendRes, regionRes, productRes]) => {
      setKPIs(kpiRes.data);
      setRevenueTrend(trendRes.data.map(d => ({
        month: d.month,
        revenue: Math.round(d.kpi_value),
      })));
      setRegionData(regionRes.data.map(d => ({
        name: d.region,
        revenue: Math.round(d.revenue),
        margin: d.margin_pct,
      })));
      setProductData(productRes.data.map(d => ({
        name: d.category,
        revenue: Math.round(d.revenue),
        margin: d.margin_pct,
      })));
      setLoading(false);
    }).catch(err => {
      console.error('Failed to load dashboard:', err);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="loading">Loading executive dashboard...</div>;

  const getKPI = (name) => {
    const kpi = kpis.find(k => k.kpi_name === name);
    return kpi ? kpi.kpi_value : 0;
  };

  const getUnit = (name) => {
    const kpi = kpis.find(k => k.kpi_name === name);
    return kpi ? kpi.kpi_unit : '';
  };

  return (
    <div>
      <h2>Executive Dashboard</h2>
      <p className="section-subtitle">RISE Inc. enterprise KPI overview — latest period</p>

      <div className="kpi-grid">
        <KPICard label="Total Revenue" value={getKPI('Total Revenue')} unit={getUnit('Total Revenue')} className="revenue" />
        <KPICard label="Gross Margin" value={getKPI('Gross Margin %')} unit={getUnit('Gross Margin %')} className="margin" />
        <KPICard label="Total Orders" value={getKPI('Order Count')} unit={getUnit('Order Count')} className="orders" />
        <KPICard label="Avg Order Value" value={getKPI('Avg Order Value')} unit={getUnit('Avg Order Value')} className="growth" />
        <KPICard label="Fulfillment Rate" value={getKPI('Fulfillment Rate')} unit={getUnit('Fulfillment Rate')} className="fulfillment" />
        <KPICard label="Headcount" value={getKPI('Headcount')} unit={getUnit('Headcount')} className="headcount" />
      </div>

      <div className="charts-row">
        <div className="chart-card full-width">
          <h4>Revenue Trend (Monthly)</h4>
          <TrendChart data={revenueTrend} dataKey="revenue" color="#10b981" unit="$" />
        </div>
      </div>

      <div className="charts-row">
        <div className="chart-card">
          <h4>Revenue by Region</h4>
          <BarChart data={regionData} dataKey="revenue" color="#3b82f6" unit="$" />
        </div>
        <div className="chart-card">
          <h4>Revenue by Product Category</h4>
          <BarChart data={productData} dataKey="revenue" color="#8b5cf6" unit="$" />
        </div>
      </div>
    </div>
  );
}

export default ExecutiveDashboard;
