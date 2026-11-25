/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 合并输入清单：合并计划一侧（分片/关联）的来源核验视图。 */
export type MergeInputSide = {
  来源名称: string
  状态: string
  已核验版本: string | null
  来源说明: string
  覆盖键: Array<string | number>
  行数: number
  可核对版本: string[]
  样例行: Array<Record<string, unknown>>
}

export type MergePreviewProof = {
  证明编号: string
  打开时间: number
  分片版本: string | null
  分片覆盖键: Array<string | number>
  关联版本: string | null
  关联覆盖键: Array<string | number>
}

export type MergePublishRecord = {
  结果: string
  时间: number
  结果批次: string | null
  分片版本: string | null
  关联版本: string | null
  待补范围: Array<string | number>
  原因?: string
}

export type MergeInputManifest = {
  计划编号?: string
  批式来源?: string
  流式来源?: string
  输入?: Record<string, MergeInputSide>
  留待核对?: Array<{ 侧: string; 来源名称: string; 版本: string; 原因: string }>
  预览证明?: MergePreviewProof | null
  已发布分片版本?: string | null
  已发布关联版本?: string | null
  待补范围?: Array<string | number>
  发布记录?: MergePublishRecord[]
  核验时间?: number
}

export type MergeInputsResult = {
  ok: boolean
  entry_id: number
  reason?: string
  deferred?: boolean
  manifest: MergeInputManifest
  proof?: MergePreviewProof
  used_versions?: { 分片版本: string | null; 关联版本: string | null }
  待补范围?: Array<string | number>
  merged_sample?: Array<Record<string, unknown>>
}
