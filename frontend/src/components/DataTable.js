function DataTable({ columns, data }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          {columns.map((col) => (
            <th key={col.key} style={col.align === 'right' ? { textAlign: 'right' } : {}}>
              {col.label}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr key={i}>
            {columns.map((col) => (
              <td key={col.key} style={col.align === 'right' ? { textAlign: 'right' } : {}}>
                {col.render ? col.render(row[col.key], row) : row[col.key]}
              </td>
            ))}
          </tr>
        ))}
        {data.length === 0 && (
          <tr>
            <td colSpan={columns.length} style={{ textAlign: 'center', color: '#94a3b8', padding: 20 }}>
              No data available
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}

export default DataTable;
