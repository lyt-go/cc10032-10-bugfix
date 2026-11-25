import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, MergeInputsResult, OverviewResult, PageResult } from '@/data/types'

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

/** 合并输入查询面板：审计只读查看已选输入与来源核验结果。 */
export async function loadMergeInputs(id: number): Promise<MergeInputsResult> {
  const response = await request(`/api/merge_plan/${id}/inputs`)
  if (!response.ok) throw new Error('合并输入清单读取失败')
  return await response.json()
}

/** 打开合并预览：为当前已核验输入留下来源证明。 */
export async function openMergePreview(id: number): Promise<MergeInputsResult> {
  const response = await request(`/api/merge_plan/${id}/preview`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return await response.json()
}

/** 发布入口：整轮采用一组已核验输入，返回实际使用版本与待补范围。 */
export async function publishMerge(id: number): Promise<MergeInputsResult> {
  const response = await request(`/api/merge_plan/${id}/publish`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return await response.json()
}
