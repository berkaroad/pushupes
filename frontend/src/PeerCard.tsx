import { Button, Card, Descriptions, Space, Tag } from 'antd'
import type { Peer } from './types'

// isOnlinePeer mirrors the cluster's own offline rule (Peer.Offline, the
// basis of Table.OnlinePeerIDs): a peer counts as reachable by clients only
// once it has announced its client-plane address AND the controller has not
// marked it down (consecutive failed liveness probes). Either condition
// renders the 离线 tag.
export function isOnlinePeer(p: Pick<Peer, 'client_addr' | 'down'>): boolean {
  return !p.down && (p.client_addr ?? '') !== ''
}

// PeerCard renders one node of the cluster's member list: id, the "当前"
// badge on the answering node, a red 离线 tag (plus a weakened dashed card)
// when the peer has no announced client address, and its three plane
// addresses ('-' where a value is missing).
//
// The footer row inside the card body carries the one membership action the
// console offers: 移除. It appears only when onRemove is passed — which the
// page does only for offline members other than this one — so the console's
// own rule matches the backend's (Engine.RemoveMember refuses an online
// member) before a click can produce a 400. The row is rendered for EVERY
// card at a fixed height (the small-button 24px line): online cards keep the
// empty slot as an invisible placeholder, so all cards in the grid stay the
// same height whether or not they carry a button.
export function PeerCard({ peer, current, slots, onRemove, removing }: {
  peer: Peer
  current: boolean
  slots: number
  onRemove?: (id: string) => void
  removing?: boolean
}) {
  const online = isOnlinePeer(peer)
  // Every card — online or offline — is the same solid bordered card: the
  // offline state is carried by the 离线 tag alone. The faded/dashed variant
  // read as a different (broken) frame style instead of a state.
  return (
    <Card size="small"
      title={<Space>{peer.id}{current ? <Tag color="blue">当前</Tag> : null}{online ? null : <Tag color="red">离线</Tag>}</Space>}
      extra={<Tag>{slots} slots</Tag>}>
      <Descriptions column={1} size="small">
        <Descriptions.Item label="Admin">{peer.admin_addr || '-'}</Descriptions.Item>
        <Descriptions.Item label="Client">{peer.client_addr || '-'}</Descriptions.Item>
        <Descriptions.Item label="Peer">{peer.peer_addr || '-'}</Descriptions.Item>
      </Descriptions>
      <div style={{ marginTop: 12, height: 24, display: 'flex', justifyContent: 'flex-end' }}>
        {onRemove
          ? <Button danger size="small" loading={removing} onClick={() => onRemove(peer.id)}>移除</Button>
          : <span style={{ visibility: 'hidden' }} aria-hidden><Button danger size="small">移除</Button></span>}
      </div>
    </Card>
  )
}
