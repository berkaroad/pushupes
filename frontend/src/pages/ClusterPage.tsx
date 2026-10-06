import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, Button, Card, Col, Form, Input, Modal, Row, Space, Statistic, Switch, Typography } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import type { ClusterStatus } from '../types'
import { addClusterNode, ensureLeader } from '../api'
import { PeerCard, isOnlinePeer } from '../PeerCard'

export default function ClusterPage() {
  const [status, setStatus] = useState<ClusterStatus | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [auto, setAuto] = useState(true)

  // The add-node modal. Membership changes are controller-only, so the submit
  // goes through addClusterNode (which resolves the leader from the CURRENT
  // status snapshot and retries once if it moved) — statusRef keeps the newest
  // table for that, the same pattern the slots page uses for migrations.
  const [adding, setAdding] = useState(false)
  const [addBusy, setAddBusy] = useState(false)
  const [aForm] = Form.useForm<{ id: string; peer_addr: string }>()
  const statusRef = useRef<ClusterStatus | null>(null)

  const refresh = useCallback(async () => {
    try {
      // ensureLeader: with several configured admins the FIRST call probes the
      // pool and pins all traffic to the Raft leader; later calls are plain
      // status polls against that pin (and re-pin when leadership moves).
      const d = await ensureLeader()
      setStatus(d)
      statusRef.current = d
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

  const doAdd = async () => {
    const { id, peer_addr } = await aForm.validateFields()
    setAddBusy(true)
    try {
      await addClusterNode(id.trim(), peer_addr.trim(), statusRef.current)
      setAdding(false)
      aForm.resetFields()
      refresh()
    } catch (e: any) {
      // Keep the modal open on failure (a wrong address is fixed in place and
      // retried); surface the backend's own message when it answers.
      const msg = e?.response?.data?.error ?? e?.message ?? String(e)
      setErr(`添加节点失败：${msg}`)
    } finally {
      setAddBusy(false)
    }
  }

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
  const peers = Object.values(status.peers)
  const online = peers.filter(isOnlinePeer).length

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Space>
        <Typography.Text>自动刷新 5s</Typography.Text>
        <Switch checked={auto} onChange={setAuto} size="small" />
      </Space>
      {err ? <Alert type="warning" closable message={err} onClose={() => setErr(null)} /> : null}
      <Row gutter={16}>
        <Col span={6}><Card><Statistic title="节点（在线 / 总数）" value={`${online} / ${peers.length}`}
          valueStyle={online < peers.length ? { color: '#cf1322' } : undefined} /></Card></Col>
        <Col span={6}><Card><Statistic title="槽位总数" value={status.slot_count} /></Card></Col>
        <Col span={6}><Card><Statistic title="迁移中槽位" value={migrating} valueStyle={{ color: migrating ? '#faad14' : undefined }} /></Card></Col>
        <Col span={6}><Card><Statistic title="Raft 状态" value={status.raft.state} valueStyle={{ color: String(status.raft.state).toLowerCase() === 'leader' ? '#3f8600' : undefined }} /></Card></Col>
      </Row>
      <Card title="节点" extra={
        <Button type="primary" size="small" icon={<PlusOutlined />} onClick={() => setAdding(true)}>
          添加节点
        </Button>
      }>
        <Row gutter={16}>
          {peers.map((p) => (
            <Col span={8} key={p.id}>
              <PeerCard peer={p} current={p.id === status.node} slots={leaderCount[p.id] ?? 0} />
            </Col>
          ))}
        </Row>
        <Typography.Text type="secondary" style={{ display: 'block', marginTop: 12 }}>
          Raft leader：<Typography.Text code>{status.raft.leader || '-'}</Typography.Text>
          {status.raft.commit_index ? <> ｜ commit <Typography.Text code>{String(status.raft.commit_index)}</Typography.Text></> : null}
        </Typography.Text>
      </Card>
      <Modal open={adding} onCancel={() => setAdding(false)} onOk={doAdd}
        okText="添加" cancelText="取消" confirmLoading={addBusy}>
        <Typography.Paragraph type="secondary" style={{ marginBottom: 12 }}>
          将节点加入运行中的集群（提交给控制器）。只需填它的 Peer（Raft）地址；
          新节点入集群后会自行上报 Admin / Client 地址。
        </Typography.Paragraph>
        <Form form={aForm} layout="vertical">
          <Form.Item name="id" label="节点 ID" rules={[{ required: true, message: '请输入节点 ID' }]}>
            <Input placeholder="如 node-4" />
          </Form.Item>
          <Form.Item name="peer_addr" label="Peer 地址" rules={[{ required: true, message: '请输入 Peer 地址' }]}>
            <Input placeholder="如 10.0.0.4:8394" />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  )
}
