import { useCallback, useEffect, useMemo, useState } from 'react'
import { Alert, Card, Col, Descriptions, Row, Space, Statistic, Switch, Tag, Typography } from 'antd'
import type { ClusterStatus } from '../types'
import { getClusterStatus } from '../api'

export default function ClusterPage() {
  const [status, setStatus] = useState<ClusterStatus | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [auto, setAuto] = useState(true)

  const refresh = useCallback(async () => {
    try {
      const d = await getClusterStatus()
      setStatus(d)
      setErr(null)
    } catch (e: any) {
      setErr(e?.message ?? String(e))
    }
  }, [])

  useEffect(() => {
    refresh()
    if (!auto) return
    const t = setInterval(refresh, 5000)
    return () => clearInterval(t)
  }, [refresh, auto])

  const leaderCount = useMemo(() => {
    if (!status) return {} as Record<string, number>
    const m: Record<string, number> = {}
    for (const p of Object.values(status.slots)) m[p.leader] = (m[p.leader] ?? 0) + 1
    return m
  }, [status])

  if (err && !status) return <Alert type="error" message="无法连接后端" description={err} />
  if (!status) return null

  const slots = Object.values(status.slots)
  const migrating = slots.filter((s) => s.state !== 'stable').length

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Space>
        <Typography.Text>自动刷新 5s</Typography.Text>
        <Switch checked={auto} onChange={setAuto} size="small" />
      </Space>
      <Row gutter={16}>
        <Col span={6}><Card><Statistic title="节点" value={Object.keys(status.peers).length} /></Card></Col>
        <Col span={6}><Card><Statistic title="槽位总数" value={status.slot_count} /></Card></Col>
        <Col span={6}><Card><Statistic title="迁移中槽位" value={migrating} valueStyle={{ color: migrating ? '#faad14' : undefined }} /></Card></Col>
        <Col span={6}><Card><Statistic title="Raft 状态" value={status.raft.state} valueStyle={{ color: String(status.raft.state).toLowerCase() === 'leader' ? '#3f8600' : undefined }} /></Card></Col>
      </Row>
      <Card title="节点">
        <Row gutter={16}>
          {Object.values(status.peers).map((p) => (
            <Col span={8} key={p.id}>
              <Card size="small" type="inner"
                title={<Space>{p.id}{p.id === status.node ? <Tag color="blue">当前</Tag> : null}</Space>}
                extra={<Tag>{leaderCount[p.id] ?? 0} slots</Tag>}>
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="Admin">{p.admin_addr}</Descriptions.Item>
                  <Descriptions.Item label="Client">{p.client_addr}</Descriptions.Item>
                  <Descriptions.Item label="Peer">{p.peer_addr}</Descriptions.Item>
                </Descriptions>
              </Card>
            </Col>
          ))}
        </Row>
        <Typography.Text type="secondary" style={{ display: 'block', marginTop: 12 }}>
          Raft leader：<Typography.Text code>{status.raft.leader || '-'}</Typography.Text>
          {status.raft.commit_index ? <> ｜ commit <Typography.Text code>{String(status.raft.commit_index)}</Typography.Text></> : null}
        </Typography.Text>
      </Card>
    </Space>
  )
}
