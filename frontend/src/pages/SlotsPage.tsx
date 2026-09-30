import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, Button, Descriptions, Drawer, Form, Input, Modal, Select, Space, Table, Tag, Typography } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import type { ClusterStatus, NodeWrites, Placement, SlotDescribe, SlotStream, SlotStreams } from '../types'
import { describeSlot, fetchNodeWrites, fetchSlotStreams, getClusterStatus, migrateSlot } from '../api'

const RATE_POLL_MS = 2000

// One slot's event-stream listing, as shown in the drawer: which node
// answered, whether that node had the slot open at all, and the pages loaded
// so far (the API pages by aggregate-id cursor).
type StreamDrawer = {
  slot: number
  addr: string
  node?: string
  loading: boolean
  loaded: boolean
  total: number
  list: SlotStream[]
  nextAfter: string
}

// humanBytes renders a byte count for the table (exact value in the title).
function humanBytes(n: number): string {
  if (n < 1024) return `${n} B`
  const units = ['KiB', 'MiB', 'GiB', 'TiB']
  let v = n / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`
}

const stateColor: Record<string, string> = {
  stable: 'green',
  migrating_out: 'orange',
  importing_in: 'gold',
  backing_up: 'blue',
}

export default function SlotsPage() {
  const [status, setStatus] = useState<ClusterStatus | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [filterState, setFilterState] = useState<string>('all')
  const [filterLeader, setFilterLeader] = useState<string>('all')
  const [detail, setDetail] = useState<{ slot: number; data?: SlotDescribe; loading: boolean } | null>(null)
  const [migrate, setMigrate] = useState<{ slot: number; placement: Placement } | null>(null)
  const [mForm] = Form.useForm<{ to_node: string }>()
  const [mBusy, setMBusy] = useState(false)
  // per-slot live write rate (evt/s), derived by diffing every node's
  // durable write counters between polls and summing across replicas
  const [rates, setRates] = useState<Record<number, number>>({})
  // slot event streams (aggregate id + latest version). The listing is served
  // from a node's in-memory slot index — no WAL file is read — so we ask the
  // nodes that hold the slot (leader, then replicas) and keep the first that
  // actually has it open.
  const [streams, setStreams] = useState<StreamDrawer | null>(null)
  const [streamQuery, setStreamQuery] = useState('')
  // per-slot gauges for the 总字节 / 事件流数量 columns, taken from the same
  // /admin/writes poll as the rates (no extra request): the node holding the
  // slot answers with real numbers, nodes that have not loaded it report 0.
  const [gauges, setGauges] = useState<Record<number, { bytes: number; streams: number }>>({})
  const prevWrites = useRef<{ at: number; perNode: Record<string, NodeWrites> } | null>(null)
  const statusRef = useRef<ClusterStatus | null>(null)

  const refresh = useCallback(async () => {
    try {
      const st = await getClusterStatus()
      statusRef.current = st
      setStatus(st)
      setErr(null)
    } catch (e: any) {
      setErr(e?.message ?? String(e))
    }
  }, [])

  useEffect(() => { refresh() }, [refresh])

  // Rate poller: uses the cached table (leaders/peers) and polls each
  // peer's /admin/writes — counters only (~33KB gz to a few KB), instead of
  // re-fetching the full 4096-slot status every 2s. A slot's client write
  // rate is its LEADER's counter delta (replica counters include
  // replication apply, which would double count); fall back to the largest
  // replica delta when the leader is unreachable.
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
        const now = Date.now()
        const prev = prevWrites.current
        prevWrites.current = { at: now, perNode }
        if (!prev) return
        const dt = (now - prev.at) / 1000
        if (dt <= 0) return
        const next: Record<number, number> = {}
        const idToAddr: Record<string, string> = {}
        for (const p of Object.values(st.peers)) idToAddr[p.id] = p.admin_addr
        for (const [slotStr, p] of Object.entries(st.slots)) {
          const s = Number(slotStr)
          const deltas: number[] = []
          const push = (addr?: string) => {
            if (!addr) return
            const cur = perNode[addr]?.writes
            const old = prev.perNode[addr]?.writes
            if (cur && old && s < cur.length && s < old.length && cur[s] >= old[s]) {
              deltas.push((cur[s] - old[s]) / dt)
            }
          }
          push(idToAddr[p.leader])
          if (deltas.length === 0) for (const r of p.replicas) push(idToAddr[r])
          if (deltas.length > 0) {
            const v = Math.max(...deltas)
            if (v > 0) next[s] = v
          }
        }
        setRates(next)

        // Gauges: take the largest on-disk footprint reported for the slot
        // (leader or replica, whichever has flushed more) together with that
        // node's stream count. Absent everywhere means nothing to show.
        const g: Record<number, { bytes: number; streams: number }> = {}
        for (const [slotStr, p] of Object.entries(st.slots)) {
          const s = Number(slotStr)
          let best: { bytes: number; streams: number } | null = null
          for (const addr of [idToAddr[p.leader], ...p.replicas.map((r) => idToAddr[r])]) {
            const m = addr ? perNode[addr] : undefined
            if (!m?.bytes || s >= m.bytes.length) continue
            const cand = { bytes: m.bytes[s] ?? 0, streams: m.streams?.[s] ?? 0 }
            if (!best || cand.bytes >= best.bytes) best = cand
          }
          if (best && best.bytes > 0) g[s] = best
        }
        setGauges(g)
      } catch {
        /* poll failure: keep last rates */
      }
    }
    tick()
    const t = setInterval(tick, RATE_POLL_MS)
    return () => { stop = true; clearInterval(t) }
  }, [])

  const rows = useMemo(() => {
    if (!status) return []
    return Object.entries(status.slots)
      .map(([id, p]) => ({ slot: Number(id), ...p }))
      .sort((a, b) => a.slot - b.slot)
      .filter((r) => (filterState === 'all' ? true : r.state === filterState))
      .filter((r) => (filterLeader === 'all' ? true : r.leader === filterLeader))
  }, [status, filterState, filterLeader])

  const filteredStreams = useMemo(() => {
    const list = streams?.list ?? []
    const q = streamQuery.trim().toLowerCase()
    return q ? list.filter((x) => x.aggregate_id.toLowerCase().includes(q)) : list
  }, [streams, streamQuery])

  // Admin addresses of the nodes holding a slot, leader first: the stream
  // listing comes from a node's own slot index, so a node that never opened
  // the slot answers loaded:false and we try the next one.
  const streamAddrs = (slot: number): string[] => {
    const st = statusRef.current
    const p = st?.slots[String(slot)]
    if (!st || !p) return []
    const seen: Record<string, boolean> = {}
    const out: string[] = []
    for (const id of [p.leader, ...p.replicas]) {
      const addr = st.peers[id]?.admin_addr
      if (addr && !seen[id]) {
        seen[id] = true
        out.push(addr)
      }
    }
    return out
  }

  const openStreams = async (slot: number) => {
    setStreamQuery('')
    const addrs = streamAddrs(slot)
    setStreams({ slot, addr: addrs[0] ?? '', loading: true, loaded: false, total: 0, list: [], nextAfter: '' })
    if (addrs.length === 0) {
      setStreams(null)
      setErr(`slot ${slot}：status 里没有该槽的 placement，无法定位持有节点`)
      return
    }
    let lastMsg = ''
    let answered: SlotStreams | null = null
    let answeredAddr = ''
    for (const addr of addrs) {
      try {
        const res = await fetchSlotStreams(addr, slot)
        answered = res
        answeredAddr = addr
        if (res.loaded) break
      } catch (e: any) {
        lastMsg = e?.response?.data?.error ?? e?.message ?? String(e)
      }
    }
    if (!answered) {
      setStreams(null)
      setErr(`slot ${slot} 事件流读取失败：${lastMsg}`)
      return
    }
    setStreams({
      slot,
      addr: answeredAddr,
      node: answered.node,
      loading: false,
      loaded: answered.loaded,
      total: answered.total,
      list: answered.streams ?? [],
      nextAfter: answered.next_after ?? '',
    })
  }

  const loadMoreStreams = async () => {
    if (!streams?.nextAfter) return
    const { addr, slot, nextAfter } = streams
    setStreams({ ...streams, loading: true })
    try {
      const res = await fetchSlotStreams(addr, slot, nextAfter)
      setStreams((cur) => cur && cur.addr === addr && cur.slot === slot
        ? { ...cur, loading: false, total: res.total, list: [...cur.list, ...(res.streams ?? [])], nextAfter: res.next_after ?? '' }
        : cur)
    } catch (e: any) {
      setStreams((cur) => (cur ? { ...cur, loading: false } : cur))
      setErr(`slot ${slot} 事件流续读失败：${e?.message ?? e}`)
    }
  }

  const openDetail = async (slot: number) => {
    setDetail({ slot, loading: true })
    try {
      const d = await describeSlot(slot, '')
      setDetail({ slot, data: d, loading: false })
    } catch (e: any) {
      setDetail(null)
      setErr(`slot ${slot} describe 失败：${e?.message ?? e}`)
    }
  }

  const doMigrate = async () => {
    if (!migrate) return
    const { to_node } = await mForm.validateFields()
    setMBusy(true)
    try {
      await migrateSlot(migrate.slot, to_node, '')
      setMigrate(null)
      refresh()
    } catch (e: any) {
      const msg = e?.response?.data?.error ?? e?.message ?? String(e)
      setErr(`迁移失败：${msg}`)
    } finally {
      setMBusy(false)
    }
  }

  if (err && !status) return <Alert type="error" message="无法连接后端" description={err} />
  if (!status) return null

  const nodeIDs = Object.keys(status.peers)
  const columns: ColumnsType<(typeof rows)[number]> = [
    { title: 'Slot', dataIndex: 'slot', width: 80, sorter: (a, b) => a.slot - b.slot },
    {
      title: '状态', dataIndex: 'state', width: 120,
      render: (v: string) => <Tag color={stateColor[v] ?? 'default'}>{v}</Tag>,
      filters: Object.keys(stateColor).map((k) => ({ text: k, value: k })),
      sorter: (a, b) => a.state.localeCompare(b.state),
    },
    { title: 'Leader', dataIndex: 'leader', width: 120, sorter: (a, b) => a.leader.localeCompare(b.leader) },
    {
      title: 'Replicas', dataIndex: 'replicas',
      render: (v: string[]) => v.map((n) => <Tag key={n}>{n}</Tag>),
    },
    { title: 'Epoch', dataIndex: 'epoch', width: 80, sorter: (a, b) => a.epoch - b.epoch },
    {
      title: '总字节', key: 'bytes', width: 110,
      sorter: (a, b) => (gauges[a.slot]?.bytes ?? 0) - (gauges[b.slot]?.bytes ?? 0),
      render: (_, r) => {
        const v = gauges[r.slot]?.bytes
        if (!v) return <Typography.Text type="secondary">-</Typography.Text>
        return <span title={`${v} 字节`}>{humanBytes(v)}</span>
      },
    },
    {
      title: '事件流数量', key: 'streams', width: 120,
      sorter: (a, b) => (gauges[a.slot]?.streams ?? 0) - (gauges[b.slot]?.streams ?? 0),
      render: (_, r) => {
        const v = gauges[r.slot]?.streams
        if (v === undefined) return <Typography.Text type="secondary">-</Typography.Text>
        return <span title={`${v} 条事件流`}>{v.toLocaleString()}</span>
      },
    },
    {
      title: (
        <div style={{ lineHeight: 1.35 }}>
          写入速率
          <br />
          <span style={{ fontSize: 12, color: 'rgba(128,128,128,.75)' }}>（evt/sec）</span>
        </div>
      ),
      key: 'rate', width: 110,
      sorter: (a, b) => (rates[a.slot] ?? 0) - (rates[b.slot] ?? 0),
      render: (_, r) => {
        const v = rates[r.slot]
        if (!v || v <= 0) return <Typography.Text type="secondary">0</Typography.Text>
        return <Tag color={v >= 100 ? 'processing' : 'default'}>{v >= 1000 ? `${(v / 1000).toFixed(1)}k` : v.toFixed(0)}</Tag>
      },
    },
    {
      title: '迁移目标', dataIndex: 'migrating_to', width: 110,
      render: (v?: string) => v ?? '-',
    },
    {
      title: '操作', key: 'ops', width: 250,
      render: (_, r) => (
        <Space>
          <Button size="small" onClick={() => openDetail(r.slot)}>详情</Button>
          <Button size="small" onClick={() => openStreams(r.slot)}>事件流</Button>
          <Button size="small" type="link" onClick={() => {
            setMigrate({ slot: r.slot, placement: { leader: r.leader, replicas: r.replicas, epoch: r.epoch, state: r.state } })
            mForm.resetFields()
          }}>迁移</Button>
        </Space>
      ),
    },
  ]

  const streamColumns: ColumnsType<SlotStream> = [
    {
      title: '聚合ID', dataIndex: 'aggregate_id', ellipsis: true,
      render: (v: string) => <Typography.Text copyable>{v}</Typography.Text>,
    },
    { title: '最新版本号', dataIndex: 'version', width: 110, align: 'right' },
  ]

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {err ? <Alert type="warning" closable message={err} onClose={() => setErr(null)} /> : null}
      <Space wrap>
        <Typography.Text>状态</Typography.Text>
        <Select value={filterState} style={{ width: 160 }} onChange={setFilterState}
          options={[{ value: 'all', label: '全部' }, ...Object.keys(stateColor).map((k) => ({ value: k, label: k }))]} />
        <Typography.Text>Leader</Typography.Text>
        <Select value={filterLeader} style={{ width: 160 }} onChange={setFilterLeader}
          options={[{ value: 'all', label: '全部' }, ...nodeIDs.map((n) => ({ value: n, label: n }))]} />
        <Button onClick={refresh}>刷新</Button>
        <Typography.Text type="secondary">共 {rows.length} / {Object.keys(status.slots).length} 槽</Typography.Text>
      </Space>
      <Table rowKey="slot" size="small" columns={columns} dataSource={rows}
        pagination={{ pageSize: 32, showSizeChanger: true, pageSizeOptions: [16, 32, 64, 128] }} />

      <Drawer open={!!detail} onClose={() => setDetail(null)} width={480}
        title={detail ? `Slot ${detail.slot}` : ''}>
        {detail?.loading ? <Typography.Text>加载中…</Typography.Text> : null}
        {detail?.data ? (
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item label="Leader">{detail.data.placement?.leader ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="Replicas">{detail.data.placement?.replicas.join(', ') ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="State">
              {detail.data.placement ? <Tag color={stateColor[detail.data.placement.state]}>{detail.data.placement.state}</Tag> : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="Epoch">{detail.data.placement?.epoch ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="HW">{detail.data.hw}</Descriptions.Item>
            <Descriptions.Item label="LastSeq">{detail.data.last_seq}</Descriptions.Item>
            <Descriptions.Item label="ISR">{detail.data.isr?.join(', ') ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="Segments">{detail.data.segments}</Descriptions.Item>
            <Descriptions.Item label="总字节">{detail.data.total_bytes}</Descriptions.Item>
          </Descriptions>
        ) : null}
      </Drawer>

      <Drawer open={!!streams} onClose={() => setStreams(null)} width={560}
        title={streams ? `Slot ${streams.slot} 事件流` : ''}>
        {streams?.loading ? <Typography.Text>加载中…</Typography.Text> : null}
        {streams && !streams.loading && !streams.loaded ? (
          <Alert type="info" showIcon message="该槽位在持有它的节点上尚未打开"
            description={<>事件流列表只读槽内索引（索引由记录<b>帧头</b>建成，不扫描 WAL 文件），因此本节点不会为它现开槽、也不做全文件扫描。持有该槽的节点（{streamAddrs(streams.slot).join('、') || '未知'}）都返回未加载：槽内可能尚无数据。</>} />
        ) : null}
        {streams?.loaded ? (
          <Space direction="vertical" size={10} style={{ width: '100%' }}>
            <Space wrap>
              <Input.Search allowClear placeholder="过滤聚合ID（仅已加载部分）" style={{ width: 250 }}
                value={streamQuery} onChange={(e) => setStreamQuery(e.target.value)} />
              <Button onClick={() => openStreams(streams.slot)}>刷新</Button>
              <Button onClick={loadMoreStreams} disabled={!streams.nextAfter}>加载更多</Button>
            </Space>
            <Typography.Text type="secondary">
              已加载 {streams.list.length} / 共 {streams.total} 条事件流 · 节点 {streams.node ?? streams.addr}
            </Typography.Text>
            <Table rowKey="aggregate_id" size="small" columns={streamColumns} dataSource={filteredStreams}
              pagination={{ pageSize: 20, showSizeChanger: false }} />
          </Space>
        ) : null}
      </Drawer>

      <Modal open={!!migrate} onCancel={() => setMigrate(null)} onOk={doMigrate}
        okText="发起迁移" okButtonProps={{ loading: mBusy }}
        title={migrate ? `迁移 Slot ${migrate.slot}（当前 leader：${migrate.placement.leader}）` : ''}>
        <Form form={mForm} layout="vertical">
          <Form.Item name="to_node" label="目标节点" rules={[{ required: true }]}>
            <Select options={nodeIDs.filter((n) => n !== migrate?.placement.leader).map((n) => ({ value: n, label: n }))} />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  )
}
