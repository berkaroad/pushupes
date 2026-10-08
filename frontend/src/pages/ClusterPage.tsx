import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, App, Button, Card, Col, Form, Input, Modal, Row, Select, Space, Statistic, Switch, Typography } from 'antd'
import { EditOutlined, PlusOutlined } from '@ant-design/icons'
import type { ClusterStatus } from '../types'
import { humanBytes } from '../format'
import { addClusterNode, ensureLeader, removeClusterNode, setReplicaPolicy, REPLICA_POLICY_LABELS, type ReplicaPolicy } from '../api'
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

  // The replica policy. The tier lives in the replicated slot table (the
  // startup flag only seeds a brand-new cluster), so this card is the
  // cluster's change entry point. It reads as a plain display until 修改 is
  // clicked: the edit state holds a DRAFT tier (nothing is sent while
  // selecting), 保存 names the layout move in a confirm dialog and only then
  // submits the one controller-only Raft command, 取消 drops the draft and
  // returns to the pre-edit display. replicaFactorFor mirrors the backend's
  // derivation (clamped to the member count) so the confirm dialog can name
  // what the replica sets will move to.
  const [policyEditing, setPolicyEditing] = useState(false)
  const [policyDraft, setPolicyDraft] = useState<ReplicaPolicy>('medium')
  const [policySaving, setPolicySaving] = useState(false)

  const replicaFactorFor = (policy: ReplicaPolicy, members: number) => {
    const n = Math.max(members, 1)
    const want = policy === 'low' ? 1 : policy === 'high' ? Math.floor((n - 1) / 2) + 1 : 2
    return Math.min(want, n)
  }

  const startPolicyEdit = () => {
    // The draft starts from what the page shows: entering the edit state is
    // never a change by itself.
    setPolicyDraft(((statusRef.current?.replica_policy ?? 'medium') as ReplicaPolicy))
    setPolicyEditing(true)
  }

  const cancelPolicyEdit = () => {
    setPolicyEditing(false)
    setPolicySaving(false)
  }

  const savePolicy = () => {
    const st = statusRef.current
    const cur = (st?.replica_policy ?? 'medium') as ReplicaPolicy
    const members = st ? Object.keys(st.peers).length : 0
    const from = st?.replica_factor ?? replicaFactorFor(cur, members)
    const to = replicaFactorFor(policyDraft, members)
    if (policyDraft === cur && to === from) {
      // Nothing to confirm: the draft matches the live tier. Close the edit
      // state instead of burning a no-op round trip.
      cancelPolicyEdit()
      return
    }
    const trend = to > from ? `副本集将补齐到每槽 ${to} 份（新席位经 fetch 追平）`
      : to < from ? `每槽副本数 ${from} → ${to}，多出的席位由控制器在保留副本与 leader 摘要等价后逐步回收`
      : `每槽副本数保持 ${to}`
    modal.confirm({
      title: `切换副本策略为 ${policyDraft}？`,
      content: (
        <div>
          <Typography.Paragraph style={{ marginBottom: 8 }}>
            {REPLICA_POLICY_LABELS[policyDraft]}。当前 {members} 个成员，{trend}。
          </Typography.Paragraph>
          <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
            策略写入 Raft 复制的槽表，全集群即时生效并持久保留；扩容副本不会搬运 leader，
            回收副本前控制器会逐槽确认留下的副本已追平。
          </Typography.Paragraph>
        </div>
      ),
      okText: '确认切换',
      cancelText: '返回编辑',
      onOk: async () => {
        setPolicySaving(true)
        try {
          const res = await setReplicaPolicy(policyDraft, statusRef.current)
          message.success(`副本策略已切换为 ${res.replica_policy}（副本因子 ${res.replica_factor}）`)
          setPolicyEditing(false)
          refresh()
        } catch (e: any) {
          // Keep the edit state on failure: the draft stays where the
          // operator can fix the cause (leader moved, unreachable admin)
          // and retry the save from here.
          const msg = e?.response?.data?.error ?? e?.message ?? String(e)
          message.error(`切换副本策略失败：${msg}`)
        } finally {
          setPolicySaving(false)
        }
      },
    })
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
      {/* The replica policy, its own card above the node block. The tier and
          the factor it derives are cluster state (replicated through the slot
          table), and changing it moves every slot's replica set — too large
          a decision to live inside a dropdown that fires on click. So it
          displays read-only until 修改 is pressed; the edit state holds a
          draft the operator can still walk away from with 取消, and only
          保存 → confirm submits the change. */}
      <Card title="副本策略" extra={!policyEditing && (
        <Button size="small" icon={<EditOutlined />} onClick={startPolicyEdit}>修改</Button>
      )}>
        {policyEditing ? (
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <Space>
              <Typography.Text type="secondary">策略</Typography.Text>
              <Select
                style={{ width: 240 }}
                value={policyDraft}
                onChange={(v) => setPolicyDraft(v as ReplicaPolicy)}
                options={(Object.keys(REPLICA_POLICY_LABELS) as ReplicaPolicy[]).map((p) => ({
                  value: p,
                  label: REPLICA_POLICY_LABELS[p],
                }))}
              />
            </Space>
            <Typography.Text type="secondary">
              副本因子按「策略 × 当前成员数（{peers.length}）」推导：low 每槽 1 份、medium 每槽 2 份、
              high 为容错节点数 + 1；保存前不会改动集群。
            </Typography.Text>
            <Space>
              <Button type="primary" loading={policySaving} onClick={savePolicy}>保存</Button>
              <Button disabled={policySaving} onClick={cancelPolicyEdit}>取消</Button>
            </Space>
          </Space>
        ) : (
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <Space size={24}>
              <Statistic title="策略" value={status.replica_policy ?? '-'} />
              <Statistic title="副本因子" value={status.replica_factor ?? '-'} suffix="份 / 槽" />
            </Space>
            <Typography.Text type="secondary">
              策略写入 Raft 复制的槽表，全集群生效并持久保留；启动参数 -replica-policy 只用于新集群的初始值，
              之后的修改都从这里提交。因子由策略与成员数推导（当前 {peers.length} 个成员），控制器每轮把副本集收敛到该因子。
            </Typography.Text>
          </Space>
        )}
      </Card>
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
