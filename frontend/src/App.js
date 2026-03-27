import { useState } from 'react';
import ExecutiveDashboard from './components/ExecutiveDashboard';
import SalesAnalytics from './components/SalesAnalytics';
import FinancialPerformance from './components/FinancialPerformance';
import WorkforceInsights from './components/WorkforceInsights';
import DataQualityMonitor from './components/DataQualityMonitor';
import './App.css';

function App() {
  const [page, setPage] = useState('executive');

  const renderPage = () => {
    switch (page) {
      case 'executive': return <ExecutiveDashboard />;
      case 'sales': return <SalesAnalytics />;
      case 'finance': return <FinancialPerformance />;
      case 'workforce': return <WorkforceInsights />;
      case 'quality': return <DataQualityMonitor />;
      default: return <ExecutiveDashboard />;
    }
  };

  return (
    <div className="app">
      <nav className="sidebar">
        <div className="logo">
          <h1>KPI Platform</h1>
          <span className="logo-subtitle">RISE Inc. Enterprise Analytics</span>
        </div>
        <ul>
          <li className={page === 'executive' ? 'active' : ''} onClick={() => setPage('executive')}>
            Executive Dashboard
          </li>
          <li className={page === 'sales' ? 'active' : ''} onClick={() => setPage('sales')}>
            Sales Analytics
          </li>
          <li className={page === 'finance' ? 'active' : ''} onClick={() => setPage('finance')}>
            Financial Performance
          </li>
          <li className={page === 'workforce' ? 'active' : ''} onClick={() => setPage('workforce')}>
            Workforce Insights
          </li>
          <li className={page === 'quality' ? 'active' : ''} onClick={() => setPage('quality')}>
            Data Quality
          </li>
        </ul>
      </nav>
      <main className="content">
        {renderPage()}
      </main>
    </div>
  );
}

export default App;
