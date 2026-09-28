import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, Button, Descriptions, Drawer, Form, Modal, Select, Space, Table, Tag, Typography } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import type { ClusterStatus, Placement, SlotDescribe } from '../types'
import { describeSlot, fetchNodeWrites, getClusterStatus, migrateSlot } from '../api'

const RATE_POLL_MS = 2000

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
  const prevWrites = useRef<{ at: number; perNode: Record<string, number[]> } | null>(null)
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
        const perNode: Record<string, number[]> = {}
        results.forEach((r, i) => {
          if (r.status === 'fulfilled' && r.value.writes) perNode[addrs[i]] = r.value.writes
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
            const cur = perNode[addr]
            const old = prev.perNode[addr]
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
    },
    { title: 'Leader', dataIndex: 'leader', width: 120 },
    {
      title: 'Replicas', dataIndex: 'replicas',
      render: (v: string[]) => v.map((n) => <Tag key={n}>{n}</Tag>),
    },
    { title: 'Epoch', dataIndex: 'epoch', width: 80 },
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
      title: '操作', key: 'ops', width: 180,
      render: (_, r) => (
        <Space>
          <Button size="small" onClick={() => openDetail(r.slot)}>详情</Button>
          <Button size="small" type="link" onClick={() => {
            setMigrate({ slot: r.slot, placement: { leader: r.leader, replicas: r.replicas, epoch: r.epoch, state: r.state } })
            mForm.resetFields()
          }}>迁移</Button>
        </Space>
      ),
    },
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
