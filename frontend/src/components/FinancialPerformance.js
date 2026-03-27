import { useState, useEffect } from 'react';
import KPICard from './KPICard';
import BarChart from './BarChart';
import DataTable from './DataTable';
import { getFinanceOverview, getFinanceVariance } from '../api';

function FinancialPerformance() {
  const [overview, setOverview] = useState(null);
  const [variance, setVariance] = useState([]);
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      getFinanceOverview(),
      getFinanceVariance(),
    ]).then(([ovRes, varRes]) => {
      setOverview(ovRes.data);
      setVariance(varRes.data);
      setLoading(false);
    }).catch(err => {
      console.error('Failed to load finance:', err);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="loading">Loading financial performance...</div>;

  const filteredVariance = filter
    ? variance.filter(v => v.account_type === filter)
    : variance;

  const fmt = (n) => `$${Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

  const varianceColumns = [
    { key: 'department', label: 'Department' },
    { key: 'account_type', label: 'Type' },
    { key: 'actual', label: 'Actual', align: 'right', render: (v) => fmt(v) },
    { key: 'budget', label: 'Budget', align: 'right', render: (v) => fmt(v) },
    { key: 'variance', label: 'Variance', align: 'right', render: (v) => (
      <span className={v >= 0 ? 'positive' : 'negative'}>{fmt(v)}</span>
    )},
    { key: 'variance_pct', label: 'Variance %', align: 'right', render: (v) => (
      <span className={v >= 0 ? 'positive' : 'negative'}>{v.toFixed(1)}%</span>
    )},
  ];

  // group revenue variance by department for chart
  const revenueByDept = variance
    .filter(v => v.account_type === 'revenue')
    .map(v => ({ name: v.department, variance: Math.round(v.variance) }));

  return (
    <div>
      <h2>Financial Performance</h2>
      <p className="section-subtitle">Budget vs actual, margins, and department-level variance</p>

      {overview && (
        <div className="kpi-grid">
          <KPICard label="Total Revenue" value={overview.total_revenue} unit="$" className="revenue" />
          <KPICard label="Total Expenses" value={overview.total_expenses} unit="$" className="neutral" />
          <KPICard label="COGS" value={overview.total_cogs} unit="$" className="neutral" />
          <KPICard label="Net Income" value={overview.net_income} unit="$"
            className={overview.net_income >= 0 ? 'revenue' : 'negative'} />
          <KPICard label="Net Margin" value={overview.net_margin_pct} unit="%"
            className={overview.net_margin_pct >= 0 ? 'margin' : 'negative'} />
          <KPICard label="Budget Variance" value={overview.budget_variance_pct} unit="%"
            className={Math.abs(overview.budget_variance_pct) < 5 ? 'fulfillment' : 'negative'} />
        </div>
      )}

      <div className="charts-row">
        <div className="chart-card full-width">
          <h4>Revenue Variance by Department</h4>
          <BarChart data={revenueByDept} dataKey="variance" color="#f59e0b" unit="$" />
        </div>
      </div>

      <h3>Budget vs Actual Detail</h3>
      <div className="filters">
        <select value={filter} onChange={(e) => setFilter(e.target.value)}>
          <option value="">All Types</option>
          <option value="revenue">Revenue</option>
          <option value="expense">Expense</option>
          <option value="cogs">COGS</option>
        </select>
      </div>
      <DataTable columns={varianceColumns} data={filteredVariance} />
    </div>
  );
}

export default FinancialPerformance;
