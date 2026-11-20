import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ──────────────────────────────────────────────────────────
// 死信复核数据包
// ──────────────────────────────────────────────────────────

export type ReviewPackage = {
  package_no: string
  payload_version: string
  exported_at: string
  item_count: number
  items: Array<Record<string, unknown>>
}

export type QualityTodo = {
  id: number
  待办编号: string
  dlq_id: number
  记录编号: string
  来源复核包: string
  载荷版本: string
  待办状态: string
  质检结论: string
  证据可用性: string
  字段证据: Array<Record<string, unknown>>
  外部结论: Array<Record<string, unknown>>
}

/** 从待审死信导出复核数据包并下载为 JSON 文件，同时返回解析后的包内容。 */
export async function exportReviewPackage(dlqIds: number[] | null): Promise<ReviewPackage> {
  const response = await request('/api/dlq-review/export', {
    method: 'POST',
    body: JSON.stringify({ dlq_ids: dlqIds }),
  })
  if (!response.ok) throw new Error('复核包导出失败')
  const blob = await response.blob()
  const text = await blob.text()
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = 'dlq-review-package.json'
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
  return JSON.parse(text) as ReviewPackage
}

/** 导入值班同事回流的复核数据包（并发幂等）。 */
export async function importReviewPackage(packageObj: unknown): Promise<Record<string, unknown>> {
  const response = await request('/api/dlq-review/import', {
    method: 'POST',
    body: JSON.stringify({ package: packageObj }),
  })
  return await response.json()
}

export async function listReviewPackages(): Promise<{ items: Array<Record<string, unknown>> }> {
  const response = await request('/api/dlq-review/packages')
  if (!response.ok) throw new Error('复核包列表读取失败')
  return await response.json()
}

export async function runQualityCheck(packageNo: string): Promise<Record<string, unknown>> {
  const response = await request('/api/dlq-review/quality-check', {
    method: 'POST',
    body: JSON.stringify({ package_no: packageNo }),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({ detail: '质量校验失败' }))
    throw new Error(String(detail.detail ?? '质量校验失败'))
  }
  return await response.json()
}

export async function listQualityTodos(status?: string): Promise<{ items: QualityTodo[] }> {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  const response = await request(`/api/dlq-review/todos${query}`)
  if (!response.ok) throw new Error('质检待办读取失败')
  return await response.json()
}

export async function closeQualityTodo(todoId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/dlq-review/todos/${todoId}/close`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({ detail: '结案失败' }))
    throw new Error(String(detail.detail ?? '结案失败'))
  }
  return await response.json()
}

export async function getReviewCase(dlqId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/dlq-review/cases/${dlqId}`)
  if (!response.ok) throw new Error('复核工作台读取失败')
  return await response.json()
}
