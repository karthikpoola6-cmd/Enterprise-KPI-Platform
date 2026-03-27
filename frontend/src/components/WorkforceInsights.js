import { useState, useEffect } from 'react';
import KPICard from './KPICard';
import BarChart from './BarChart';
import TrendChart from './TrendChart';
import DataTable from './DataTable';
import { getWorkforceOverview, getWorkforceByDepartment, getKPITrends } from '../api';

function WorkforceInsights() {
  const [overview, setOverview] = useState(null);
  const [departments, setDepartments] = useState([]);
  const [headcountTrend, setHeadcountTrend] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      getWorkforceOverview(),
      getWorkforceByDepartment(),
      getKPITrends('Headcount', 'overall'),
    ]).then(([ovRes, deptRes, trendRes]) => {
      setOverview(ovRes.data);
      setDepartments(deptRes.data);
      setHeadcountTrend(trendRes.data.map(d => ({
        month: d.month,
        headcount: d.kpi_value,
      })));
      setLoading(false);
    }).catch(err => {
      console.error('Failed to load workforce:', err);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="loading">Loading workforce insights...</div>;

  const fmt = (n) => `$${Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

  const deptColumns = [
    { key: 'department', label: 'Department' },
    { key: 'headcount', label: 'Headcount', align: 'right' },
    { key: 'new_hires', label: 'New Hires', align: 'right' },
    { key: 'terminations', label: 'Terminations', align: 'right' },
    { key: 'turnover_rate', label: 'Turnover %', align: 'right', render: (v) => (
      <span className={v > 10 ? 'negative' : 'positive'}>{v.toFixed(1)}%</span>
    )},
    { key: 'avg_salary', label: 'Avg Salary', align: 'right', render: (v) => fmt(v) },
  ];

  return (
    <div>
      <h2>Workforce Insights</h2>
      <p className="section-subtitle">Headcount, turnover, salary costs, and department-level staffing</p>

      {overview && (
        <div className="kpi-grid">
          <KPICard label="Total Headcount" value={overview.total_headcount} unit="count" className="headcount" />
          <KPICard label="New Hires (Month)" value={overview.new_hires} unit="count" className="revenue" />
          <KPICard label="Terminations" value={overview.terminations} unit="count"
            className={overview.terminations > 5 ? 'negative' : 'neutral'} />
          <KPICard label="Turnover Rate" value={overview.turnover_rate} unit="%"
            className={overview.turnover_rate > 10 ? 'negative' : 'fulfillment'} />
          <KPICard label="Avg Salary" value={overview.avg_salary} unit="$" className="growth" />
          <KPICard label="Revenue / Employee" value={overview.revenue_per_employee} unit="$" className="margin" />
        </div>
      )}

      <div className="charts-row">
        <div className="chart-card">
          <h4>Headcount Trend</h4>
          <TrendChart data={headcountTrend} dataKey="headcount" color="#06b6d4" />
        </div>
        <div className="chart-card">
          <h4>Headcount by Department</h4>
          <BarChart
            data={departments.map(d => ({ name: d.department, headcount: d.headcount }))}
            dataKey="headcount" color="#8b5cf6"
          />
        </div>
      </div>

      <h3>Department Breakdown</h3>
      <DataTable columns={deptColumns} data={departments} />
    </div>
  );
}

export default WorkforceInsights;
