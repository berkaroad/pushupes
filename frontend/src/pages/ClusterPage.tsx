import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, App, Button, Card, Col, Form, Input, InputNumber, Modal, Row, Select, Space, Statistic, Switch, Typography } from 'antd'
import { EditOutlined } from '@ant-design/icons'
import type { ClusterStatus, FlowControlDetail, NodeWrites } from '../types'
import { humanBytes, humanBytesPerSec } from '../format'
import { clearFlowControl, ensureLeader, fetchFlowControl, fetchNodeWrites, setReplicaPolicy, setFlowControl, REPLICA_POLICY_LABELS, type ReplicaPolicy } from '../api'
import { PeerCard, isOnlinePeer } from '../PeerCard'
import type { NodeRatePoint } from '../NodeRateChart'

// NODE_RATE_POINTS is the card chart's ring size: 60 samples x 2s = the
// same two-minute window the slot drawer's rate chart keeps.
const NODE_RATE_POINTS = 60

// The write-speed cards sample every node's durable counters on the same
// cadence the slots page polls them at (2s): the rate is the diff between two
// samples of the leaders-only totals.
const WRITES_POLL_MS = 2000

// leaderWriteTotals sums one counter field (durable writes, or their bytes)
// over every slot AT ITS LEADER (per-slot arrays are indexed by slot id):
// replicas apply replicated records into their own counters and a migrating
// slot advances counters on both ends until it lands, so counting a slot only
// at its placement's current leader counts each record exactly once — the
// same one-leader rule the backend's storage gauge follows. It answers null
// when any ledged slot's leader did not report the number (unreachable node,
// or an older node without the field): a partial sum would diff against the
// last full one into a fake dip, so the caller keeps its previous rate.
function leaderWriteTotals(
  st: ClusterStatus, perNode: Record<string, NodeWrites>, field: 'writes' | 'write_bytes',
): number | null {
  const idToAddr: Record<string, string> = {}
  for (const p of Object.values(st.peers)) idToAddr[p.id] = p.admin_addr
  let total = 0
  for (const [slotStr, p] of Object.entries(st.slots)) {
    if (!p.leader) continue
    const s = Number(slotStr)
    const arr = perNode[idToAddr[p.leader]]?.[field]
    if (!arr || s >= arr.length) return null
    total += arr[s]
  }
  return total
}

export default function ClusterPage() {
  const [status, setStatus] = useState<ClusterStatus | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [auto, setAuto] = useState(true)

  // Membership changes — adds AND removals — are operator CLI actions
  // (POST / DELETE /admin/cluster/nodes, see DESIGN.md): the console
  // deliberately offers no membership buttons. statusRef keeps the newest
  // table for the policy command the console DOES own, the same pattern the
  // slots page uses for migrations.
  const statusRef = useRef<ClusterStatus | null>(null)

  const { modal, message } = App.useApp()

  // Flow control (node-local). The card tag reads the config view the 2s
  // writes poll already carries per node (configured=true → 流控 tag); the
  // modal is opened per card, prefills from that node's own
  // GET /admin/flow-control (the poll view has no duration string), and
  // posts DELETE-or-POST semantics straight to THAT node — no controller,
  // no redirect, whichever node answers is the node it governs.
  const [flowConfigured, setFlowConfigured] = useState<Record<string, boolean>>({})
  const flowConfiguredRef = useRef<Record<string, boolean>>({})
  // flowFiring marks nodes whose 流控 tag blinks: set when a 2s poll sees
  // ANY growth in that node's per-slot hit arrays (the throttle fired in
  // the window), and cleared 3s later. Sustained throttling grows the
  // counters on every successive sample, which re-arms the timer — the
  // tag keeps blinking while refusals keep landing and settles 3s after
  // the last one. Timers live in a ref map so a re-arm cancels cleanly.
  const [flowFiring, setFlowFiring] = useState<Record<string, boolean>>({})
  const flowFiringTimers = useRef<Record<string, ReturnType<typeof setTimeout>>>({})
  // per-node last snapshot of flow_control_hits, keyed by admin addr.
  const lastFlowHits = useRef<Record<string, number[]>>({})
  const fireFlow = (id: string) => {
    setFlowFiring((m) => (m[id] ? m : { ...m, [id]: true }))
    clearTimeout(flowFiringTimers.current[id])
    flowFiringTimers.current[id] = setTimeout(() => {
      setFlowFiring((m) => {
        const { [id]: _drop, ...rest } = m
        return rest
      })
    }, 3000)
  }
  const [flowTarget, setFlowTarget] = useState<{ id: string; addr: string } | null>(null)
  const [flowBusy, setFlowBusy] = useState(false)
  const [fForm] = Form.useForm<{ tokens: number; period: string }>()

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

  // The write-speed cards: the cluster's durable write rate, msg/s and bytes/s.
  // It rides the same light /admin/writes poll the slots page uses — every
  // peer answers its per-slot counters, and leaderWriteTotals folds the table
  // into ONE number per field by summing each slot at its leader. Two
  // consecutive samples diff into the rate; the first sample (and any sample
  // whose leaders were not all reporting) keeps the last known rate.
  const [writeRate, setWriteRate] = useState<{ msg: number; bytes: number } | null>(null)
  const lastSample = useRef<{ at: number; writes: number; bytes: number } | null>(null)

  // 存储数量: the records stored cluster-wide, summed at each slot's LEADER
  // (the same one-leader fold the rate uses — the leader's durable write
  // counter IS the seq count of that copy, replicas would double count).
  // The card shows '-' until the poll sees one complete sample.
  const [storedRecords, setStoredRecords] = useState<number | null>(null)

  // The per-node card charts: same poll, same one-leader rule applied to
  // ONE node's led slots only (the card answers "what lands on this node
  // as a leader", which is also the face flow control governs). Ring
  // buffers of NODE_RATE_POINTS samples (2 min at 2s, the drawer's window);
  // a sample is skipped when the node's leaders did not all report both
  // counters — a partial total would diff into a fake jump or dip. The
  // series lives in state (cards re-render as it grows), the per-node last
  // sample and a mirror ref in refs, so the 2s tick only pushes state when
  // some node actually gained a sample.
  const [nodeRates, setNodeRates] = useState<Record<string, NodeRatePoint[]>>({})
  const nodeRatesRef = useRef<Record<string, NodeRatePoint[]>>({})
  const lastNodeSample = useRef<Record<string, { at: number; writes: number; bytes: number }>>({})

  useEffect(() => {
    let stop = false
    const tick = async () => {
      try {
        const st = statusRef.current
        if (!st) return
        const addrs = Object.values(st.peers).map((p) => p.admin_addr)
        const results = await Promise.allSettled(addrs.map((a) => fetchNodeWrites(a)))
        if (stop) return
        const perNode: Record<string, NodeWrites> = {}
        results.forEach((r, i) => {
          if (r.status === 'fulfilled' && r.value.writes) perNode[addrs[i]] = r.value
        })
        // Flow-control card tags: one configured flag per node id, from the
        // config view this same poll carries. A node that did not answer
        // keeps its last known state (the tag says "governed", not "alive").
        const fc: Record<string, boolean> = {}
        for (const p of Object.values(st.peers)) {
          const m = perNode[p.admin_addr]
          fc[p.id] = m ? !!m.flow_control?.configured : (flowConfiguredRef.current[p.id] ?? false)
          // Fire detection: any growth in this node's per-slot refusal
          // counters since ITS last sample means a token-less write was
          // rejected at one of the slots it leads. The budget is node-wide
          // but the counts stay per-slot, so a plain array scan sees every
          // fire anywhere on the node. First sample only stores, never
          // fires (the counters survive restarts? no — a restart resets
          // them, but a page load against a throttled node must not blink
          // on the baseline it has no history of).
          const hits = m?.flow_control_hits
          if (hits && hits.length) {
            const before = lastFlowHits.current[p.admin_addr]
            lastFlowHits.current[p.admin_addr] = hits
            if (before && hits.some((h, i) => (before[i] ?? 0) < h)) fireFlow(p.id)
          }
        }
        flowConfiguredRef.current = fc
        setFlowConfigured(fc)
        // A sample counts for the rate only when BOTH totals are complete
        // (every ledged slot answered its writes AND its bytes at its
        // leader). Skipping an incomplete sample rather than storing it
        // keeps the diff windows honest: a partial sum held as "last" would
        // turn the next full sum into a fake jump, and the reverse a dip.
        // Per-node totals for the card charts (see nodeRates): sum each
        // node's LED slots only; skip the node when any led slot's counter
        // array is missing or short. A node that leads nothing sums 0/0 —
        // the flat line is the truthful picture of "no led writes".
        const nowMs = Date.now()
        const prevNode = lastNodeSample.current
        const nextNode: Record<string, { at: number; writes: number; bytes: number }> = {}
        let ratesChanged = false
        const nr = { ...nodeRatesRef.current }
        for (const p of Object.values(st.peers)) {
          let w = 0
          let b = 0
          let ok = true
          for (const [slotStr, sp] of Object.entries(st.slots)) {
            if (sp.leader !== p.id) continue
            const m = perNode[p.admin_addr]
            const s = Number(slotStr)
            if (!m?.writes || s >= m.writes.length || !m.write_bytes || s >= m.write_bytes.length) {
              ok = false
              break
            }
            w += m.writes[s]
            b += m.write_bytes[s]
          }
          if (!ok) continue
          nextNode[p.id] = { at: nowMs, writes: w, bytes: b }
          const prev = prevNode[p.id]
          if (!prev || nowMs <= prev.at) continue
          const dt = (nowMs - prev.at) / 1000
          // Counters only move forward; a slot changing leaders retargets
          // which node answers for it, so clamp negatives (one flat sample
          // at the boundary beats a fake dip).
          const arr = [...(nr[p.id] ?? []), {
            t: nowMs,
            msg: Math.max(0, (w - prev.writes) / dt),
            kib: Math.max(0, (b - prev.bytes) / dt / 1024),
          }]
          nr[p.id] = arr.length > NODE_RATE_POINTS ? arr.slice(arr.length - NODE_RATE_POINTS) : arr
          ratesChanged = true
        }
        lastNodeSample.current = nextNode
        if (ratesChanged) {
          nodeRatesRef.current = nr
          setNodeRates(nr)
        }

        const writes = leaderWriteTotals(st, perNode, 'writes')
        const bytes = leaderWriteTotals(st, perNode, 'write_bytes')
        // The 存储数量 card rides this same leader-only sum: it updates on any
        // complete writes sample, even while bytes is still incomplete.
        if (writes !== null) setStoredRecords(writes)
        if (writes === null || bytes === null) return
        const prev = lastSample.current
        const now = Date.now()
        lastSample.current = { at: now, writes, bytes }
        if (!prev || now <= prev.at) return
        const dt = (now - prev.at) / 1000
        // Counters only move forward; a leader change can retarget which
        // node answers for a slot mid-poll, so clamp negatives to 0.
        setWriteRate({
          msg: Math.max(0, (writes - prev.writes) / dt),
          bytes: Math.max(0, (bytes - prev.bytes) / dt),
        })
      } catch {
        /* poll failure: keep the last rate */
      }
    }
    tick()
    const t = setInterval(tick, WRITES_POLL_MS)
    const firingTimers = flowFiringTimers.current
    return () => { stop = true; clearInterval(t); Object.values(firingTimers).forEach(clearTimeout) }
  }, [])

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

  // 推导口径与后端 cluster.ReplicaCountForPolicy 一致：low=1 份、medium=2 份、
  // high=每个数据节点一份全量（因子就是数据节点数），并按数据节点数钳制。
  const replicaFactorFor = (policy: ReplicaPolicy, dataNodes: number) => {
    const n = Math.max(dataNodes, 1)
    const want = policy === 'low' ? 1 : policy === 'high' ? n : 2
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
    // 因子按数据面池推导（后端 Table.HostingIDs）：voter 不承载副本，
    // 不进这个数。
    const dataNodes = st
      ? Object.values(st.peers).filter((p) => p.role === 'learner' && p.client_addr).length
      : 0
    const from = st?.replica_factor ?? replicaFactorFor(cur, dataNodes)
    const to = replicaFactorFor(policyDraft, dataNodes)
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
            {REPLICA_POLICY_LABELS[policyDraft]}。当前 {dataNodes} 个数据节点，{trend}。
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

  // openFlowControl opens the per-node modal prefilled from the node's own
  // GET /admin/flow-control: the poll's config view carries the numbers but
  // not the duration STRING the POST body round-trips, so one detail read
  // (cheap, one node, on click) seeds the form instead of guessing units.
  const openFlowControl = async (id: string) => {
    const addr = statusRef.current?.peers?.[id]?.admin_addr
    if (!addr) {
      message.warning(`节点 ${id} 没有可用的 admin 地址`)
      return
    }
    setFlowTarget({ id, addr })
    fForm.resetFields()
    try {
      const d: FlowControlDetail = await fetchFlowControl(addr)
      fForm.setFieldsValue({
        tokens: d.unlimited ? undefined : d.tokens,
        period: d.unlimited ? undefined : (d.period && d.period !== '0s' ? d.period : undefined),
      })
    } catch {
      // A node the console cannot read still accepts a fresh config: open
      // the form empty and let the operator fill it.
    }
  }

  const doFlowSave = async () => {
    if (!flowTarget) return
    const { tokens, period } = await fForm.validateFields()
    setFlowBusy(true)
    try {
      const n = Number(tokens)
      if (!n || n <= 0 || !period) {
        // The form's own zero/empty rule is the clear path: DELETE matches
        // "POST 0 tokens" in the backend and reads cleaner in its answer.
        await clearFlowControl(flowTarget.addr)
        message.success(`节点 ${flowTarget.id} 已恢复不限流`)
      } else {
        await setFlowControl(flowTarget.addr, n, String(period).trim())
        message.success(`节点 ${flowTarget.id} 流控已设置：节点 ${n} 条 / ${String(period).trim()}（全部 leader 槽共享）`)
      }
      setFlowTarget(null)
    } catch (e: any) {
      // Keep the modal open on failure (the period string is the usual
      // culprit and is fixed in place).
      const msg = e?.response?.data?.error ?? e?.message ?? String(e)
      message.error(`节点 ${flowTarget.id} 流控设置失败：${msg}`)
    } finally {
      setFlowBusy(false)
    }
  }

  const doFlowClear = async () => {
    if (!flowTarget) return
    setFlowBusy(true)
    try {
      await clearFlowControl(flowTarget.addr)
      message.success(`节点 ${flowTarget.id} 已恢复不限流`)
      setFlowTarget(null)
    } catch (e: any) {
      const msg = e?.response?.data?.error ?? e?.message ?? String(e)
      message.error(`节点 ${flowTarget.id} 清除流控失败：${msg}`)
    } finally {
      setFlowBusy(false)
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

  const peers = Object.values(status.peers)
  // 数据面池规模——副本因子就是按这个数推导的（后端 Table.HostingIDs 口径：
  // 自报 learner 角色、且已上报 client 地址的成员）。
  const dataNodes = peers.filter((p) => p.role === 'learner' && p.client_addr).length
  // The summary row splits the fleet by plane: control nodes are the raft
  // voters, data nodes the learners; each card counts online members with the
  // cluster's own offline rule (isOnlinePeer).
  const controlPeers = peers.filter((p) => p.role === 'voter')
  const dataPeers = peers.filter((p) => p.role === 'learner')
  const controlOnline = controlPeers.filter(isOnlinePeer).length
  const dataOnline = dataPeers.filter(isOnlinePeer).length
  // The cluster storage sample: every slot counted once, at its leader. A node
  // that does not serve the field yet (or has not sampled) renders '-'.
  const storageBytes = status.storage_bytes
  const storageLowerBound = storageBytes !== undefined && status.storage_bytes_complete === false
  // The write-speed cards show '-' until the poller has two complete samples.
  const writeSpeedMsg = writeRate ? `${Math.round(writeRate.msg)} msg/s` : '-'
  const writeSpeedBytes = writeRate ? humanBytesPerSec(writeRate.bytes) : '-'

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Space>
        <Typography.Text>自动刷新 5s</Typography.Text>
        <Switch checked={auto} onChange={setAuto} size="small" />
      </Space>
      {err ? <Alert type="warning" closable message={err} onClose={() => setErr(null)} /> : null}
      {/* The summary row: four cards fill the 24-column grid (span 6 each).
          The two write-speed cards take a row of their own (span 12 each):
          count and size are read side by side, never crammed into one value. */}
      <Row gutter={16}>
        <Col span={6}><Card><Statistic title="控制节点" value={`${controlOnline} / ${controlPeers.length}`}
          valueStyle={controlOnline < controlPeers.length ? { color: '#cf1322' } : undefined} /></Card></Col>
        <Col span={6}><Card><Statistic title="数据节点" value={`${dataOnline} / ${dataPeers.length}`}
          valueStyle={dataOnline < dataPeers.length ? { color: '#cf1322' } : undefined} /></Card></Col>
        <Col span={6}><Card><Statistic title="存储数量" value={storedRecords ?? '-'} /></Card></Col>
        <Col span={6}><Card><Statistic title="存储大小"
          value={storageBytes === undefined ? '-' : humanBytes(storageBytes)}
          valueStyle={storageLowerBound ? { color: '#faad14' } : undefined}
          suffix={storageLowerBound ? '（下界）' : undefined} /></Card></Col>
      </Row>
      <Row gutter={16}>
        <Col span={12}><Card><Statistic title="写入速度（数量）" value={writeSpeedMsg} /></Card></Col>
        <Col span={12}><Card><Statistic title="写入速度（大小）" value={writeSpeedBytes} /></Card></Col>
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
              副本因子按「策略 × 数据节点数（{dataNodes}）」推导：low 每槽 1 份、medium 每槽 2 份（默认）、
              high 每个数据节点一份全量；保存前不会改动集群。
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
              之后的修改都从这里提交。因子由策略与数据节点数推导（当前 {dataNodes} 个数据节点），控制器每轮把副本集收敛到该因子。
            </Typography.Text>
          </Space>
        )}
      </Card>
      {/* The node directory split by plane. Control on top: voters get a row
          of TWO PLAIN cards (same span as the data cards below) — no rate
          chart and no flow control (a voter never leads writes; flow control
          is a data-plane face) — and the answering node keeps its 当前 tag
          here, where it lives. Below it the
          data plane: learners get two full cards per row, and 当前 is gone
          from them (the answering node is a control member). Neither block
          carries membership actions: add and remove are CLI operations. */}
      <Card title="控制节点">
        <Row gutter={[16, 16]}>
          {controlPeers.map((p) => (
            <Col span={12} key={p.id}>
              <PeerCard peer={p} current={p.id === status.node} slots={leaderCount[p.id] ?? 0} plain />
            </Col>
          ))}
        </Row>
        <Typography.Text type="secondary" style={{ display: 'block', marginTop: 12 }}>
          Raft leader：<Typography.Text code>{status.raft.leader || '-'}</Typography.Text>
          {status.raft.commit_index ? <> ｜ commit <Typography.Text code>{String(status.raft.commit_index)}</Typography.Text></> : null}
        </Typography.Text>
      </Card>
      <Card title="数据节点">
        <Row gutter={[16, 16]}>
          {dataPeers.map((p) => (
            <Col span={12} key={p.id}>
              <PeerCard peer={p} slots={leaderCount[p.id] ?? 0}
                flowControlConfigured={flowConfigured[p.id] ?? false}
                flowControlFiring={!!flowFiring[p.id]}
                ratePoints={nodeRates[p.id]}
                onFlowControl={isOnlinePeer(p) ? openFlowControl : undefined} />
            </Col>
          ))}
        </Row>
      </Card>
      {/* The per-node flow-control modal. Node-local: saving posts to the
          target node's own admin plane, never the controller. The body
          spells out what the numbers mean — ONE node-wide token bucket of
          N per period, shared by every slot THIS node leads; a write
          without a token fails 1006 FlowControl. Empty/0 on either field
          means unlimited and is saved as a DELETE. */}
      <Modal open={!!flowTarget} onCancel={() => setFlowTarget(null)}
        title={`节点流控：${flowTarget?.id ?? ''}`} width={460}
        footer={[
          <Button key="clear" onClick={doFlowClear} loading={flowBusy}>恢复不限流</Button>,
          <Button key="cancel" onClick={() => setFlowTarget(null)}>取消</Button>,
          <Button key="save" type="primary" loading={flowBusy} onClick={doFlowSave}>保存</Button>,
        ]}>
        <Typography.Paragraph type="secondary" style={{ marginBottom: 12 }}>
          仅作用于本节点（重启后回到不限流）：该节点作为 leader 的所有槽共享一个令牌桶，
          任一槽的写入取不到令牌即报 <Typography.Text code>1006 FlowControl</Typography.Text>，
          被取走的令牌在「限流周期」到期后归还。
        </Typography.Paragraph>
        <Form form={fForm} layout="vertical">
          <Form.Item name="tokens" label="节点令牌数（tokens）">
            <InputNumber min={1} style={{ width: '100%' }} placeholder="如 100；留空或 0 = 不限流" />
          </Form.Item>
          <Form.Item name="period" label="限流周期（period）"
            extra={'Go 时长字符串，如 "1s"、"500ms"、"1m"。节点稳态写入上限 = 令牌数 / 周期（跨该节点全部 leader 槽）。'}>
            <Input placeholder="如 1s；留空 = 不限流" />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  )
}
