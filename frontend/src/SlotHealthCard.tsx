import { Button, Card, Space, Statistic, Tooltip, Typography } from 'antd'
import { CheckCircleFilled, CloseCircleOutlined, LoadingOutlined, ReloadOutlined } from '@ant-design/icons'
import type { SlotHealth } from './types'

// The 槽位健康 card: the counted verdict of the slot-copy walk (the same
// standard cmd/slotcheck prints — one shared implementation, no drift). By
// operator call the card shows COUNTS only; the per-slot detail stays in the
// CLI. An all-zero round collapses to a single green 健康; only non-zero
// kinds appear, damaged-or-worse in red with a cross, and behind — a catch-up
// in flight, transient by definition — in the amber the page already uses for
// "not yet settled". A failed round keeps showing the last good numbers next
// to its error: stale health beats a blank card.

// Display order and the Chinese/English bridge: the label is Chinese like the
// rest of the page, the tooltip carries the slotcheck vocabulary and the
// one-line standard for that kind.
const HEALTH_KINDS = [
  { key: 'diverged', en: 'diverged', label: '分叉', color: '#ff4d4f',
    desc: '副本 LEO 不低于 leader、目录却更短——复制追平修不好，需显式重建（明细用 slotcheck 查）' },
  { key: 'behind', en: 'behind', label: '落后', color: '#faad14',
    desc: '副本 LEO 低于 leader，正在追平——瞬态' },
  { key: 'not_loaded', en: 'not-loaded', label: '未加载', color: '#ff4d4f',
    desc: '该副本尚未在此节点打开——拉取循环会把它开起来' },
  { key: 'unreachable', en: 'unreachable', label: '不可达', color: '#ff4d4f',
    desc: '该副本节点未应答本轮检测' },
] as const

export function SlotHealthCard({ health, onRecheck }: {
  health: SlotHealth | undefined
  onRecheck: () => void
}) {
  const count = (key: 'diverged' | 'behind' | 'not_loaded' | 'unreachable') =>
    (health?.[key] ?? 0) as number
  const bad = HEALTH_KINDS.filter((k) => count(k.key) > 0)
  const at = health?.scanned_at
    ? new Date(health.scanned_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    : null
  return (
    <Card title="槽位健康" extra={
      <Button size="small" icon={<ReloadOutlined />} loading={health?.state === 'scanning'}
        onClick={onRecheck}>重新检测</Button>
    }>
      {!health || health.state === 'idle' || (health.state === 'scanning' && !health.scanned_at) ? (
        <Typography.Text type="secondary">
          {health?.state === 'scanning' ? <LoadingOutlined style={{ marginRight: 6 }} /> : null}
          检测中…
        </Typography.Text>
      ) : health.state === 'error' && !health.scanned_at ? (
        <Typography.Text type="danger">检测失败：{health.error || '未应答'}</Typography.Text>
      ) : bad.length === 0 ? (
        <>
          <Statistic value="健康" prefix={<CheckCircleFilled />} valueStyle={{ color: '#52c41a' }} />
          <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
            {health.compared ?? 0} 槽已对比{at ? ` · ${at}` : ''}
          </Typography.Text>
        </>
      ) : (
        <>
          <Space size={24} wrap>
            {bad.map((k) => (
              <Tooltip key={k.key} title={`${k.en} — ${k.desc}`}>
                <Typography.Text style={{ color: k.color, fontWeight: 600, fontSize: 16 }}>
                  <CloseCircleOutlined /> {k.label} {count(k.key)}
                </Typography.Text>
              </Tooltip>
            ))}
          </Space>
          <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
            {health.compared ?? 0} 槽已对比{at ? ` · ${at}` : ''}
          </Typography.Text>
        </>
      )}
      {health?.state === 'error' && health.scanned_at ? (
        <Typography.Text type="danger" style={{ display: 'block', marginTop: 8 }}>
          检测失败：{health.error || '未应答'}（显示 {at} 的最近结果）
        </Typography.Text>
      ) : null}
    </Card>
  )
}
