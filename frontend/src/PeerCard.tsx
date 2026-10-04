import { Card, Descriptions, Space, Tag } from 'antd'
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
export function PeerCard({ peer, current, slots }: { peer: Peer; current: boolean; slots: number }) {
  const online = isOnlinePeer(peer)
  return (
    <Card size="small" type="inner"
      style={online ? undefined : { borderStyle: 'dashed', opacity: 0.75 }}
      title={<Space>{peer.id}{current ? <Tag color="blue">当前</Tag> : null}{online ? null : <Tag color="red">离线</Tag>}</Space>}
      extra={<Tag>{slots} slots</Tag>}>
      <Descriptions column={1} size="small">
        <Descriptions.Item label="Admin">{peer.admin_addr || '-'}</Descriptions.Item>
        <Descriptions.Item label="Client">{peer.client_addr || '-'}</Descriptions.Item>
        <Descriptions.Item label="Peer">{peer.peer_addr || '-'}</Descriptions.Item>
      </Descriptions>
    </Card>
  )
}
