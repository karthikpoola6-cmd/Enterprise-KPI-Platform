import { useState, useEffect } from 'react';
import KPICard from './KPICard';
import DataTable from './DataTable';
import { getQualityLatest, getQualityHistory, getPipelineStatus, runPipeline } from '../api';

function DataQualityMonitor() {
  const [checks, setChecks] = useState([]);
  const [history, setHistory] = useState([]);
  const [pipeline, setPipeline] = useState([]);
  const [running, setRunning] = useState(false);
  const [loading, setLoading] = useState(true);

  const loadData = () => {
    Promise.all([
      getQualityLatest(),
      getQualityHistory(),
      getPipelineStatus(),
    ]).then(([checkRes, histRes, pipeRes]) => {
      setChecks(checkRes.data);
      setHistory(histRes.data);
      setPipeline(pipeRes.data);
      setLoading(false);
    }).catch(err => {
      console.error('Failed to load quality:', err);
      setLoading(false);
    });
  };

  useEffect(() => { loadData(); }, []);

  const handleRunPipeline = () => {
    setRunning(true);
    runPipeline().then(() => {
      setRunning(false);
      loadData();
    }).catch(err => {
      console.error('Pipeline failed:', err);
      setRunning(false);
    });
  };

  if (loading) return <div className="loading">Loading data quality monitor...</div>;

  const passed = checks.filter(c => c.status === 'pass').length;
  const warnings = checks.filter(c => c.status === 'warning').length;
  const failed = checks.filter(c => c.status === 'fail').length;
  const total = checks.length;

  const lastRun = pipeline.length > 0 ? pipeline[pipeline.length - 1] : null;
  const pipelineOk = lastRun && lastRun.status === 'completed';

  const checkColumns = [
    { key: 'check_name', label: 'Check' },
    { key: 'table_name', label: 'Table' },
    { key: 'layer', label: 'Layer' },
    { key: 'status', label: 'Status', render: (v) => (
      <span className={`badge badge-${v}`}>{v.toUpperCase()}</span>
    )},
    { key: 'records_checked', label: 'Checked', align: 'right', render: (v) => v.toLocaleString() },
    { key: 'records_failed', label: 'Failed', align: 'right', render: (v) => v.toLocaleString() },
    { key: 'failure_rate', label: 'Failure %', align: 'right', render: (v) => `${v.toFixed(2)}%` },
  ];

  const pipelineColumns = [
    { key: 'step_name', label: 'Step' },
    { key: 'status', label: 'Status', render: (v) => (
      <span className={`badge badge-${v === 'completed' ? 'pass' : v === 'failed' ? 'fail' : 'warning'}`}>
        {v.toUpperCase()}
      </span>
    )},
    { key: 'records_processed', label: 'Records', align: 'right', render: (v) => v.toLocaleString() },
    { key: 'duration_seconds', label: 'Duration', align: 'right', render: (v) => `${v.toFixed(2)}s` },
  ];

  return (
    <div>
      <h2>Data Quality Monitor</h2>
      <p className="section-subtitle">Pipeline status, quality checks, and data governance</p>

      <div className={`pipeline-banner ${pipelineOk ? 'success' : lastRun ? 'failed' : 'unknown'}`}>
        <div>
          <strong>Pipeline Status:</strong>{' '}
          {pipelineOk ? 'Last run completed successfully' : lastRun ? 'Last run had issues' : 'No runs recorded'}
          {lastRun && <span style={{ color: '#64748b', marginLeft: 12 }}>Run ID: {lastRun.run_id}</span>}
        </div>
        <button className="run-btn" onClick={handleRunPipeline} disabled={running}>
          {running ? 'Running...' : 'Run Pipeline'}
        </button>
      </div>

      <div className="kpi-grid">
        <KPICard label="Total Checks" value={total} unit="count" className="headcount" />
        <KPICard label="Passed" value={passed} unit="count" className="quality-pass" />
        <KPICard label="Warnings" value={warnings} unit="count" className="quality-warn" />
        <KPICard label="Failed" value={failed} unit="count" className="quality-fail" />
      </div>

      <h3>Latest Pipeline Run</h3>
      <DataTable columns={pipelineColumns} data={pipeline} />

      <h3>Quality Check Results</h3>
      <DataTable columns={checkColumns} data={checks} />

      {history.length > 0 && (
        <>
          <h3>Run History</h3>
          <DataTable
            columns={[
              { key: 'run_id', label: 'Run ID' },
              { key: 'total_checks', label: 'Total', align: 'right' },
              { key: 'passed', label: 'Passed', align: 'right' },
              { key: 'warnings', label: 'Warnings', align: 'right' },
              { key: 'failed', label: 'Failed', align: 'right' },
              { key: 'run_at', label: 'Time' },
            ]}
            data={history}
          />
        </>
      )}
    </div>
  );
}

export default DataQualityMonitor;
