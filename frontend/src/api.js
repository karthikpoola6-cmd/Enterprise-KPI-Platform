import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:8000/api',
});

// KPIs
export const getKPIs = () => api.get('/kpis');
export const getKPITrends = (kpiName, dimension) =>
  api.get('/kpis/trends', { params: { kpi_name: kpiName, dimension } });

// Sales
export const getSalesSummary = (params = {}) => api.get('/sales/summary', { params });
export const getSalesByRegion = () => api.get('/sales/by-region');
export const getSalesByProduct = () => api.get('/sales/by-product');
export const getSalesBySegment = () => api.get('/sales/by-customer-segment');

// Finance
export const getFinanceOverview = () => api.get('/finance/overview');
export const getFinanceVariance = (accountType) =>
  api.get('/finance/variance', { params: accountType ? { account_type: accountType } : {} });

// Workforce
export const getWorkforceOverview = () => api.get('/workforce/overview');
export const getWorkforceByDepartment = () => api.get('/workforce/by-department');

// Quality
export const getQualityLatest = () => api.get('/quality/latest');
export const getQualityHistory = () => api.get('/quality/history');

// Pipeline
export const getPipelineStatus = () => api.get('/pipeline/status');
export const runPipeline = () => api.post('/pipeline/run');

export default api;
