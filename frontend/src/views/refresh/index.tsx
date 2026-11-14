import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/refresh'
const columns = [
  "策略编号",
  "目标数据集",
  "刷新周期",
  "刷新范围",
  "增量字段",
  "全量标记",
  "下一次刷新",
  "刷新水位",
  "策略状态"
]
// 启用入口只对已创建开放，生效中显示暂停入口，已暂停显示关闭入口，已关闭关闭全部入口
const ACTIONS_BY_STATUS: Record<string, string[]> = {
  '已创建': ['启用刷新'],
  '生效中': ['暂停刷新'],
  '已暂停': ['关闭刷新'],
  '已关闭': [],
}

type Row = Record<string, string | number | null>

export default function RefreshView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const stats = useMemo(() => ([
    { label: '刷新策略总数', value: total },
    { label: '生效中策略', value: rows.filter((row) => row['策略状态'] === '生效中').length },
    { label: '已暂停刷新', value: rows.filter((row) => row['策略状态'] === '已暂停').length },
  ]), [rows, total])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('刷新策略列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据定时刷新列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      const payload = await response.json().catch(() => null)
      if (!response.ok) {
        const detail = payload && typeof payload.detail === 'string' ? payload.detail : '数据定时刷新动作未生效，请稍后重试'
        throw new Error(detail)
      }
      // 越级操作会返回失败回执：展示回执并重新拉取，清掉页面上可能残留的错位状态
      if (payload && payload.ok === false) {
        setErrorMessage(payload.message ?? '动作被拦截')
        await reload()
        return
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据定时刷新操作失败')
      await reload()
    }
  }

  return (
    <section className="page" data-module="refresh">
      <header className="page-head">
        <div>
          <h2>数据定时刷新管理</h2>
          <p className="page-desc">维护数据刷新的周期配置、范围控制与增量策略，为数据定时刷新提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('刷新策略登记入口尚未接入审批流')}>
            登记刷新策略
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出数据定时刷新清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const allowedActions = ACTIONS_BY_STATUS[String(row['策略状态'] ?? '')] ?? []
            return (
              <tr key={String(row.id)}>
                {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                <td className="row-actions">
                  {allowedActions.length === 0 && <span className="empty-actions">—</span>}
                  {allowedActions.map((action) => (
                    <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                      {action}
                    </button>
                  ))}
                </td>
              </tr>
            )
          })}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无数据定时刷新数据，可先登记刷新策略</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条数据定时刷新记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
