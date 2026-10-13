import { Button, Card, Descriptions, Space, Tag } from 'antd'
import { ControlOutlined } from '@ant-design/icons'
import type { Peer } from './types'
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
// per-node /admin/writes poll the page already runs.
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
export function PeerCard({ peer, current, slots, flowControlConfigured, flowControlFiring, onFlowControl, ratePoints, plain }: {
  peer: Peer
  current?: boolean
  slots: number
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
      title={<Space>{peer.id}{current ? <Tag color="blue">当前</Tag> : null}{online ? null : <Tag color="red">离线</Tag>}{!plain && flowControlConfigured ? <Tag color="orange" className={flowControlFiring ? 'flow-blink' : undefined}>流控</Tag> : null}</Space>}
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
