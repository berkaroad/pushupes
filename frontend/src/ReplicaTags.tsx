import { Tag, Tooltip, Typography } from 'antd'

// A slot's replica set, rendered as tags. The leader keeps the coloured tag
// (the one distinction the table needs); a replica whose local copy is queued
// for AUTOMATIC CLEANUP after a migration is shown weakened instead — the
// cleanup hint rides a tooltip.
//
// The weakening is deliberately theme-agnostic: the node name goes through
// Typography's secondary text colour (a design token, not a hardcoded grey) and
// the tag is slightly transparent, so it reads as "fading out" in both light
// and dark themes.

// dropHint is the tooltip text for a replica whose local copy is scheduled to
// be dropped, or '' when nothing is queued. dropAtUnix is the unix second the
// node itself reported (0/undefined = not queued); `now` exists for tests.
export function dropHint(node: string, dropAtUnix: number | undefined, now = Date.now()): string {
  if (!dropAtUnix) return ''
  const left = Math.max(0, Math.round((dropAtUnix * 1000 - now) / 1000))
  const mins = Math.floor(left / 60)
  const secs = left % 60
  const at = new Date(dropAtUnix * 1000).toLocaleTimeString('zh-CN', { hour12: false })
  return `迁移后待自动清理：${node} 上这个槽的本地副本已排队，预计 ${at} 删除（约 ${mins} 分 ${secs} 秒后）`
}

export function ReplicaTags({ replicas, leader, dropAt, surplus, now }: {
  replicas: string[]
  leader: string
  // node id -> unix second at which that node drops its local copy (absent =
  // not queued). Only the nodes that reported one appear here: a node that
  // did not answer the poll simply is not marked. The backend only ever reports
  // a node that is NO LONGER part of the replica set — a node that is still a
  // member never queues its copy, so it is never marked.
  dropAt?: Record<string, number>
  // Copies of this slot queued for cleanup that have left the replica set, so
  // they are not in `replicas` any more. They are shown next to the set (a
  // surplus copy is not a member) and weakened like any other queued copy.
  surplus?: string[]
  now?: number
}) {
  const ts = now ?? Date.now()
  return (
    <>
      {replicas.map((n) => {
        const hint = dropHint(n, dropAt?.[n], ts)
        if (hint) {
          return (
            <Tooltip key={n} title={hint}>
              <Tag style={{ opacity: 0.55 }}>
                <Typography.Text type="secondary">{n}</Typography.Text>
              </Tag>
            </Tooltip>
          )
        }
        return n === leader ? <Tag key={n} color="gold">{n}</Tag> : <Tag key={n}>{n}</Tag>
      })}
      {(surplus ?? []).map((n) => {
        const hint = dropHint(n, dropAt?.[n], ts)
        if (!hint) return null
        return (
          <Tooltip key={`surplus-${n}`} title={hint}>
            <Tag style={{ opacity: 0.55 }}>
              <Typography.Text type="secondary">{n}</Typography.Text>
            </Tag>
          </Tooltip>
        )
      })}
    </>
  )
}
