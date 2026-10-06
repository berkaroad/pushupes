import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, App, Button, Card, Col, Form, Input, Modal, Row, Space, Statistic, Switch, Typography } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import type { ClusterStatus } from '../types'
import { humanBytes } from '../format'
import { addClusterNode, ensureLeader, removeClusterNode } from '../api'
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

  // The remove-node action. The confirm modal names the target; removal is
  // offline-only on BOTH sides: the button reaches the page only for offline
  // peers (PeerCard gets onRemove only when !isOnlinePeer), and the backend
  // re-checks the replicated table (Engine.RemoveMember refuses an online
  // member with a 400) — a stale 5s snapshot must never be the only gate.
  // removingId marks the in-flight target so only its card shows a spinner.
  const { modal, message } = App.useApp()
  const [removingId, setRemovingId] = useState<string | null>(null)

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

  const askRemove = (id: string) => {
    modal.confirm({
      title: `确认移除节点 ${id}？`,
      content: (
        <div>
          该操作把 <Typography.Text code>{id}</Typography.Text> 从 Raft
          成员列表移除，仅离线节点允许。移除后控制器会把它的槽主迁到存活副本并自动补齐副本集；
          节点进程本身不受影响，若之后重新拉起会再次自动报名入集群。
        </div>
      ),
      okText: '移除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: () => doRemove(id),
    })
  }

  const doRemove = async (id: string) => {
    // Guard the offline rule again at action time (from the freshest snapshot),
    // not only at render time: auto-refresh may have raced a node back online
    // between the paint and the click.
    const peer = statusRef.current?.peers?.[id]
    if (peer && isOnlinePeer(peer)) {
      message.warning(`节点 ${id} 当前在线，只能移除离线节点`)
      return
    }
    setRemovingId(id)
    try {
      await removeClusterNode(id, statusRef.current)
      message.success(`节点 ${id} 已提交移除`)
      refresh()
    } catch (e: any) {
      // The backend's own reason (still online / not the controller after two
      // tries) is the actionable text; show it instead of a generic failure.
      const msg = e?.response?.data?.error ?? e?.message ?? String(e)
      message.error(`移除节点 ${id} 失败：${msg}`)
    } finally {
      setRemovingId(null)
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
  // The cluster storage sample: every slot counted once, at its leader. A node
  // that does not serve the field yet (or has not sampled) renders '-'.
  const storageBytes = status.storage_bytes
  const storageLowerBound = storageBytes !== undefined && status.storage_bytes_complete === false

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Space>
        <Typography.Text>自动刷新 5s</Typography.Text>
        <Switch checked={auto} onChange={setAuto} size="small" />
      </Space>
      {err ? <Alert type="warning" closable message={err} onClose={() => setErr(null)} /> : null}
      {/* Four cards, one row of the 24-column grid. (A fifth would not divide
          it evenly, so it would drop onto a line of its own.) */}
      <Row gutter={16}>
        <Col span={6}><Card><Statistic title="节点（在线 / 总数）" value={`${online} / ${peers.length}`}
          valueStyle={online < peers.length ? { color: '#cf1322' } : undefined} /></Card></Col>
        <Col span={6}><Card><Statistic title="槽位总数" value={status.slot_count} /></Card></Col>
        <Col span={6}><Card><Statistic title="迁移中槽位" value={migrating} valueStyle={{ color: migrating ? '#faad14' : undefined }} /></Card></Col>
        <Col span={6}><Card><Statistic title="存储大小"
          value={storageBytes === undefined ? '-' : humanBytes(storageBytes)}
          valueStyle={storageLowerBound ? { color: '#faad14' } : undefined}
          suffix={storageLowerBound ? '（下界）' : undefined} /></Card></Col>
      </Row>
      <Card title="节点" extra={
        <Button type="primary" size="small" icon={<PlusOutlined />} onClick={() => setAdding(true)}>
          添加节点
        </Button>
      }>
        {/* [16, 16]: the second value is the row gap — without it cards that
            wrap onto a second line touch the row above edge to edge. */}
        <Row gutter={[16, 16]}>
          {peers.map((p) => (
            <Col span={8} key={p.id}>
              {/* 移除 only on offline cards, never on this answering node — the
                  same offline rule the backend enforces, rendered before the
                  click instead of failing inside it. */}
              <PeerCard peer={p} current={p.id === status.node} slots={leaderCount[p.id] ?? 0}
                onRemove={isOnlinePeer(p) || p.id === status.node ? undefined : askRemove}
                removing={removingId === p.id} />
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
