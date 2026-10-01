import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, Button, Descriptions, Drawer, Form, Input, Modal, Select, Space, Table, Tag, Typography } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import type { ClusterStatus, NodeWrites, Placement, SlotDescribe, SlotStream, SlotStreams } from '../types'
import { fetchNodeWrites, fetchSlotDescribe, fetchSlotStreams, getClusterStatus, migrateSlot } from '../api'
import { RATE_POINTS, RateChart, type RatePoint } from '../RateChart'
import { ReplicaTags, dropHint } from '../ReplicaTags'

const RATE_POLL_MS = 2000

// surplusCopies lists the nodes that reported a queued cleanup for a slot but
// are no longer part of its replica set: they are the copies the console must
// show weakened NEXT TO the set. The backend never queues a copy a slot still
// counts on, so a node that is still a member never shows up here.
function surplusCopies(replicas: string[], dropAt?: Record<string, number>): string[] {
  if (!dropAt) return []
  return Object.keys(dropAt).filter((n) => !replicas.includes(n))
}

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
  const [detail, setDetail] = useState<{ slot: number; addr: string; data?: SlotDescribe; loading: boolean } | null>(null)
  const [migrate, setMigrate] = useState<{ slot: number; placement: Placement } | null>(null)
  const [mForm] = Form.useForm<{ to_node: string }>()
  const [mBusy, setMBusy] = useState(false)
  // slot event streams (aggregate id + latest version). The listing is served
  // from a node's in-memory slot index — no WAL file is read — so we ask the
  // nodes that hold the slot (leader, then replicas) and keep the first that
  // actually has it open.
  const [streams, setStreams] = useState<StreamDrawer | null>(null)
  const [streamQuery, setStreamQuery] = useState('')
  // write-rate history of the slot the detail drawer has open (evts/sec per 2s
  // sample); sampled from the slot's leader counter, replicas count their
  // replication applies too and would double count.
  const [rateSeries, setRateSeries] = useState<RatePoint[]>([])
  // per-slot WAL bytes for the 总字节 column, taken from the same /admin/writes
  // poll as the counters (no extra request): the node holding the slot answers
  // with real numbers, a node that has not loaded it reports 0.
  const [slotBytes, setSlotBytes] = useState<Record<number, number>>({})
  // Per-slot post-migration cleanup queue, node id -> unix second the node
  // drops its own copy at. Filled from the same per-node poll as the counters
  // and gauges above: each node reports only its OWN queue, so a node that did
  // not answer simply leaves its replicas unmarked.
  const [dropAt, setDropAt] = useState<Record<number, Record<string, number>>>({})
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

  // Gauge poller: uses the cached table (leaders/peers) and polls each peer's
  // /admin/writes — per-slot counters and gauges only, instead of re-fetching
  // the full slot-table status every 2s. The write rate is no longer a column:
  // the detail drawer samples the opened slot's own counter (see below).
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
        const idToAddr: Record<string, string> = {}
        for (const p of Object.values(st.peers)) idToAddr[p.id] = p.admin_addr
        const addrToId: Record<string, string> = {}
        for (const p of Object.values(st.peers)) if (p.admin_addr) addrToId[p.admin_addr] = p.id

        // 总字节: the largest on-disk footprint reported for the slot (leader
        // or replica, whichever has flushed more). Absent everywhere means
        // nothing to show.
        const g: Record<number, number> = {}
        for (const [slotStr, p] of Object.entries(st.slots)) {
          const s = Number(slotStr)
          let best = 0
          for (const addr of [idToAddr[p.leader], ...p.replicas.map((r) => idToAddr[r])]) {
            const m = addr ? perNode[addr] : undefined
            if (!m?.bytes || s >= m.bytes.length) continue
            if (m.bytes[s] > best) best = m.bytes[s]
          }
          if (best > 0) g[s] = best
        }
        setSlotBytes(g)

        // Cleanup queue: each node reports its own per-slot drop deadlines, so
        // the marking is per (slot, node). Only nodes that answered contribute;
        // a node that did not is simply not marked.
        const dd: Record<number, Record<string, number>> = {}
        for (const [addr, m] of Object.entries(perNode)) {
          const id = addrToId[addr]
          if (!id || !m.dropping) continue
          m.dropping.forEach((at, s) => {
            if (at > 0) dd[s] = { ...(dd[s] ?? {}), [id]: at }
          })
        }
        setDropAt(dd)
      } catch {
        /* poll failure: keep the last gauges */
      }
    }
    tick()
    const t = setInterval(tick, RATE_POLL_MS)
    return () => { stop = true; clearInterval(t) }
  }, [])

  useEffect(() => {
    const slot = detail?.slot
    const addr = detail?.addr
    if (slot === undefined || !addr) return
    let last: { at: number; counter: number } | null = null
    const tick = async () => {
      // Field table: re-read the slot from the node that answered, so HW /
      // LastSeq / 总字节 track the writes instead of freezing at open time.
      try {
        const d = await fetchSlotDescribe(addr, slot)
        setDetail((cur) => (cur && cur.slot === slot ? { ...cur, data: d } : cur))
      } catch {
        /* keep the last answer: a transient failure should not blank the view */
      }
      // Chart: the slot leader's durable counter (replica counters include
      // replication applies and would double count).
      const st = statusRef.current
      const p = st?.slots[String(slot)]
      const addrs = [p?.leader, ...(p?.replicas ?? [])]
        .map((id) => (st && id ? st.peers[id]?.admin_addr : undefined))
        .filter((a): a is string => !!a)
      for (const a of addrs) {
        try {
          const m = await fetchNodeWrites(a)
          const counter = m.writes?.[slot]
          if (counter === undefined) continue
          const now = Date.now()
          if (last && counter >= last.counter) {
            const dt = (now - last.at) / 1000
            if (dt > 0) {
              const v = (counter - last.counter) / dt
              setRateSeries((prev) => [...prev, { t: now, v }].slice(-RATE_POINTS))
            }
          }
          last = { at: now, counter }
          return
        } catch {
          /* holder unreachable: try the next one */
        }
      }
    }
    tick()
    const id = setInterval(tick, RATE_POLL_MS)
    return () => clearInterval(id)
  }, [detail?.slot, detail?.addr])

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

  // The slot's leader admin address ('' when the table has no leader for it).
  const leaderAdminAddr = (slot: number): string => {
    const st = statusRef.current
    const p = st?.slots[String(slot)]
    return (p && st ? st.peers[p.leader]?.admin_addr : undefined) ?? ''
  }

  // placementOf narrows the optional placement for the drawer's render.
  const placementOf = (d?: SlotDescribe): Placement | null => d?.placement ?? null

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
    setRateSeries([])
    setDetail({ slot, addr: '', loading: true })
    // Always the slot's leader: describe answers with the asked node's own
    // numbers, and the leader is the only node with the whole picture (it
    // tracks HW from replica progress). A replica would answer with its own,
    // possibly lagging copy.
    const addr = leaderAdminAddr(slot)
    if (!addr) {
      setDetail(null)
      setErr(`slot ${slot} 详情读取失败：status 里没有该槽的 leader 地址`)
      return
    }
    try {
      const d = await fetchSlotDescribe(addr, slot)
      setDetail({ slot, addr, data: d, loading: false })
    } catch (e: any) {
      setDetail(null)
      setErr(`slot ${slot} 详情读取失败：${e?.response?.data?.error ?? e?.message ?? String(e)}`)
    }
  }


  const doMigrate = async () => {
    if (!migrate) return
    const { to_node } = await mForm.validateFields()
    setMBusy(true)
    try {
      // A migration is a controller-only (Raft leader) command, and the
      // console's /api proxy may be fronting a follower — a follower refuses
      // instead of forwarding. migrateSlot resolves the controller's admin
      // address from the status table the console already holds (no extra
      // request) and retries once against a freshly read table if the leader
      // moved between the read and the request.
      await migrateSlot(migrate.slot, to_node, statusRef.current)
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
    {
      // The replication set carries the role: the leader's tag is the coloured
      // one (border + fill), no legend needed. A copy of the slot queued for
      // automatic cleanup after a migration is weakened — its copy is on its
      // way out (hover for the expected time). Only a copy that has LEFT the
      // replica set is ever queued, so it is rendered next to the set rather
      // than inside it: a member never gets weakened.
      title: 'Replicas', dataIndex: 'replicas',
      render: (v: string[], r) =>
        <ReplicaTags replicas={v} leader={r.leader} dropAt={dropAt[r.slot]}
          surplus={surplusCopies(v, dropAt[r.slot])} />,
    },
    {
      title: '总字节', key: 'bytes', width: 110,
      sorter: (a, b) => (slotBytes[a.slot] ?? 0) - (slotBytes[b.slot] ?? 0),
      render: (_, r) => {
        const v = slotBytes[r.slot]
        if (!v) return <Typography.Text type="secondary">-</Typography.Text>
        return <span title={`${v} 字节`}>{humanBytes(v)}</span>
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
            <Descriptions.Item label="Replicas">
              {placementOf(detail.data)
                ? (() => {
                    const p = placementOf(detail.data)!
                    // the set, plus any copy of this slot queued for cleanup
                    // that has already left it: the backend never queues a copy
                    // the slot still counts on, so a member cannot appear here.
                    const queued = surplusCopies(p.replicas, dropAt[detail.slot])
                    return (
                      <>
                        <ReplicaTags replicas={p.replicas} leader={p.leader}
                          dropAt={dropAt[detail.slot]} surplus={queued} />
                        {[...p.replicas, ...queued]
                          .map((n) => dropHint(n, dropAt[detail.slot]?.[n]))
                          .filter((h) => h)
                          .map((h) => (
                            <div key={h}><Typography.Text type="secondary">{h}</Typography.Text></div>
                          ))}
                      </>
                    )
                  })()
                : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="State">
              {placementOf(detail.data) ? <Tag color={stateColor[placementOf(detail.data)!.state]}>{placementOf(detail.data)!.state}</Tag> : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="Epoch">{placementOf(detail.data)?.epoch ?? '-'}</Descriptions.Item>
            <Descriptions.Item label="HW">{detail.data.hw}</Descriptions.Item>
            <Descriptions.Item label="LastSeq">{detail.data.last_seq}</Descriptions.Item>
            <Descriptions.Item label="Segments">{detail.data.segments}</Descriptions.Item>
            <Descriptions.Item label="总字节">
              <span title={`${detail.data.total_bytes} 字节`}>{humanBytes(detail.data.total_bytes)}</span>
            </Descriptions.Item>
          </Descriptions>
        ) : null}
        {detail?.data ? (
          <div style={{ marginTop: 16 }}>
            <Typography.Text strong>写入速率</Typography.Text>
            <RateChart points={rateSeries} />
          </div>
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
