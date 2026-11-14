import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/etl'
const columns = [
  "任务编号",
  "任务名称",
  "输入源",
  "转换步骤",
  "输出目标",
  "调度周期",
  "依赖关系",
  "任务状态",
  "当前批次"
]
// 启动入口只对草稿开放，运行中显示暂停入口，已暂停显示归档入口，完成态关闭全部入口
const ACTIONS_BY_STATUS: Record<string, string[]> = {
  '草稿': ['启动任务'],
  '运行中': ['暂停任务'],
  '已暂停': ['归档任务'],
  '已完成': [],
}

type Row = Record<string, string | number | null>

export default function EtlView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const stats = useMemo(() => ([
    { label: '准备任务总数', value: total },
    { label: '运行中任务', value: rows.filter((row) => row['任务状态'] === '运行中').length },
    { label: '已暂停任务', value: rows.filter((row) => row['任务状态'] === '已暂停').length },
  ]), [rows, total])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('准备任务列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据准备流水线列表读取失败')
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
        const detail = payload && typeof payload.detail === 'string' ? payload.detail : '数据准备流水线动作未生效，请稍后重试'
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
      setErrorMessage(error instanceof Error ? error.message : '数据准备流水线操作失败')
      await reload()
    }
  }

  return (
    <section className="page" data-module="etl">
      <header className="page-head">
        <div>
          <h2>数据准备流水线管理</h2>
          <p className="page-desc">维护数据准备的转换步骤、调度配置与依赖管理，为数据准备流水线提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('准备任务登记入口尚未接入审批流')}>
            登记准备任务
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出数据准备流水线清单
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
            const allowedActions = ACTIONS_BY_STATUS[String(row['任务状态'] ?? '')] ?? []
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
              <td className="empty-state" colSpan={columns.length + 1}>暂无数据准备流水线数据，可先登记准备任务</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条数据准备流水线记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
