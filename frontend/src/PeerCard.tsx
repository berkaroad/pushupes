import { Button, Card, Descriptions, Space, Tag, Tooltip } from 'antd'
import { ControlOutlined } from '@ant-design/icons'
import type { Peer, PendingSlotView } from './types'
import { NodeRateChart, type NodeRatePoint } from './NodeRateChart'

// isOnlinePeer mirrors the cluster's own offline rule (Peer.Offline, the
// basis of Table.OnlinePeerIDs): a peer counts as reachable by clients only
// once it has announced its client-plane address AND the controller has not
// marked it down (consecutive failed liveness probes). Either condition
// renders the 离线 tag.
export function isOnlinePeer(p: Pick<Peer, 'client_addr' | 'down'>): boolean {
  return !p.down && (p.client_addr ?? '') !== ''
}

// The orange 流控 tag marks a node with flow control CONFIGURED (writes to
// the slots it leads are governed by one node-wide token bucket) — the
// presence of the config. flowControlFiring blinks it (see flow-blink in
// index.css): the page detected refusals on this node within its last 2s
// poll sample, and the blink holds for 3s after the last fire — a glance
// answers "governed" versus "actively throttling". Both flags come from the
// per-node /admin/status poll the page already runs.
//
// The footer row inside the card body carries the one node action the
// console offers: 流控 (open this node's flow-control modal). Membership
// actions live on the operator's command line (POST / DELETE
// /admin/cluster/nodes, see DESIGN.md), never on a card. 流控 is node-local
// and harmless to open on any live node, so it appears whenever
// onFlowControl is passed; the row is rendered at a fixed height (the
// small-button 24px line) with an invisible placeholder per absent button,
// so all data cards in the grid stay the same height. A plain (control-
// plane) card has neither action, so it renders no row at all.
export function PeerCard({ peer, current, slots, syncing, fenced, flowControlConfigured, flowControlFiring, onFlowControl, ratePoints, plain }: {
  peer: Peer
  current?: boolean
  slots: number
  // syncing is the count of slots THIS node leads whose replica sets are
  // still short of in-sync (led − synced off the node's own synced_slots
  // poll) — pending on the leader's replicas, the per-card view of the
  // sync). 0 (or an unanswered node) renders no tag.
  syncing?: number
  // fenced is THIS node's pending local copies (pending_slots off the same
  // poll): copies the ownership verdict fences — writes to them are refused
  // until an operator adopts them. The red tag is louder than 同步中 on
  // purpose: this blocks client writes, catch-up does not. The tooltip
  // lists slot + verdict, capped at ten rows with a total line beyond that
  // (a degraded fleet can fence hundreds of copies; the card stays one tag
  // wide). Empty/absent renders no tag.
  fenced?: PendingSlotView[]
  flowControlConfigured?: boolean
  flowControlFiring?: boolean
  onFlowControl?: (id: string) => void
  ratePoints?: NodeRatePoint[]
  plain?: boolean
}) {
  const online = isOnlinePeer(peer)
  // Every card — online or offline — is the same solid bordered card: the
  // offline state is carried by the 离线 tag alone. The faded/dashed variant
  // read as a different (broken) frame style instead of a state.
  return (
    <Card size="small"
      title={<Space>{peer.id}{current ? <Tag color="blue">当前</Tag> : null}{online ? null : <Tag color="red">离线</Tag>}{!plain && flowControlConfigured ? <Tag color="orange" className={flowControlFiring ? 'flow-blink' : undefined}>流控</Tag> : null}{!plain && syncing ? <Tooltip title={`作为 leader 有 ${syncing} 个槽的副本尚未同步到位（加节点 / 重排 / 追平期间），追平后消失`}><Tag color="orange">同步中 {syncing}</Tag></Tooltip> : null}{!plain && fenced && fenced.length > 0 ? <Tooltip title={<div>{fenced.slice(0, 10).map((v) => <div key={v.slot}>槽 {v.slot}：{v.reason}</div>)}{fenced.length > 10 ? <div>… 共 {fenced.length} 个槽</div> : null}</div>}><Tag color="red">已围栏 {fenced.length}</Tag></Tooltip> : null}</Space>}
      extra={plain ? null : <Tag>{slots} slots</Tag>}>
      <Descriptions column={1} size="small">
        <Descriptions.Item label="Admin">{peer.admin_addr || '-'}</Descriptions.Item>
        <Descriptions.Item label="Client">{peer.client_addr || '-'}</Descriptions.Item>
        <Descriptions.Item label="Peer">{peer.peer_addr || '-'}</Descriptions.Item>
      </Descriptions>
      {!plain && (
        <div style={{ marginTop: 12, height: 24, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          {onFlowControl
            ? <Button size="small" icon={<ControlOutlined />} onClick={() => onFlowControl(peer.id)}>流控</Button>
            : <span style={{ visibility: 'hidden' }} aria-hidden><Button size="small">流控</Button></span>}
        </div>
      )}
      {/* The node's own write-rate history (msg/s + KiB/s, dual axis): what
          this node lands as a LEADER, the same face flow control governs.
          The card chart is the per-node mirror of the cluster write-speed
          cards above — fed by the page's existing 2s writes poll. A plain
          (control-plane) card never leads writes, so it carries no chart. */}
      {!plain && (
        <div style={{ marginTop: 12 }}>
          <NodeRateChart points={ratePoints ?? []} />
        </div>
      )}
    </Card>
  )
}
