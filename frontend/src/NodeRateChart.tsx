import { Space, Typography, theme } from 'antd'
import { fmtRate, fmtTime } from './RateChart'

// A node-card sample: one 2s window of the node's leader-slot write rate,
// carried as a count rate (msg/s) AND a byte rate (KiB/s). Both axes share
// the x (time); each line scales against its OWN peak — the two units are
// not comparable on one scale, which is why the chart is dual-axis.
export type NodeRatePoint = { t: number; msg: number; kib: number }

const W = 440
const H = 150
const PAD = { left: 42, right: 46, top: 16, bottom: 22 }

// NodeRateChart overlays two polylines on one time axis (plain SVG, the
// console's existing no-chart-dependency rule): the left ticks carry the
// msg/s line (primary colour), the right ticks carry the KiB/s line
// (orange). The caption names each line's latest value in its own colour so
// the legend and the axis roles live in the same place.
export function NodeRateChart({ points }: { points: NodeRatePoint[] }) {
  const { token } = theme.useToken()
  if (points.length < 2) {
    return (
      <Typography.Text type="secondary">
        采集中…（每 2s 采样一次该节点 leader 槽的 durable 计数，两个样本后成图）
      </Typography.Text>
    )
  }
  const plotW = W - PAD.left - PAD.right
  const plotH = H - PAD.top - PAD.bottom
  const peakMsg = Math.max(1, ...points.map((p) => p.msg))
  const peakKib = Math.max(1, ...points.map((p) => p.kib))
  const stepX = plotW / (points.length - 1)
  const xAt = (i: number) => PAD.left + i * stepX
  const yMsg = (v: number) => PAD.top + (1 - v / peakMsg) * plotH
  const yKib = (v: number) => PAD.top + (1 - v / peakKib) * plotH
  const polyAt = (key: 'msg' | 'kib', yAt: (v: number) => number) =>
    points.map((p, i) => `${xAt(i).toFixed(1)},${yAt(p[key]).toFixed(1)}`).join(' ')
  const last = points[points.length - 1]

  const n = points.length - 1
  const xticks = [...new Set(n >= 30
    ? [0, Math.floor(n / 3), Math.floor((2 * n) / 3), n]
    : n >= 2 ? [0, Math.floor(n / 2), n] : [0, n])]
  const MSG = token.colorPrimary
  const KIB = '#fa8c16' // the 流控 tag's orange: the second series, not the throttle
  return (
    <Space direction="vertical" size={4} style={{ width: '100%' }}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} role="img"
        aria-label="节点写入速度折线图（左轴 msg/s，右轴 KiB/s，横轴时间点）">
        {[0, peakMsg / 2, peakMsg].map((v) => (
          <text key={v} x={PAD.left - 6} y={yMsg(v) + 3.5} fontSize={10} textAnchor="end"
            fill={MSG}>{fmtRate(v)}</text>
        ))}
        {[0, peakKib / 2, peakKib].map((v) => (
          <text key={v} x={W - PAD.right + 6} y={yKib(v) + 3.5} fontSize={10} textAnchor="start"
            fill={KIB}>{fmtRate(v)}</text>
        ))}
        {[0, peakMsg / 2, peakMsg].map((v) => (
          <line key={v} x1={PAD.left} y1={yMsg(v)} x2={W - PAD.right} y2={yMsg(v)}
            stroke={token.colorBorderSecondary} strokeDasharray={v === 0 ? undefined : '3 3'} />
        ))}
        <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={H - PAD.bottom} stroke={token.colorBorder} />
        <line x1={W - PAD.right} y1={PAD.top} x2={W - PAD.right} y2={H - PAD.bottom} stroke={KIB} />
        <line x1={PAD.left} y1={H - PAD.bottom} x2={W - PAD.right} y2={H - PAD.bottom} stroke={token.colorBorder} />
        <text x={PAD.left} y={10} fontSize={10} fill={MSG}>msg/s</text>
        <text x={W - PAD.right} y={10} fontSize={10} textAnchor="end" fill={KIB}>KiB/s</text>
        <polyline points={polyAt('kib', yKib)} fill="none" stroke={KIB} strokeWidth={1.5} />
        <polyline points={polyAt('msg', yMsg)} fill="none" stroke={MSG} strokeWidth={2} />
        <circle cx={xAt(n).toFixed(1)} cy={yMsg(last.msg).toFixed(1)} r={3} fill={MSG} />
        <circle cx={xAt(n).toFixed(1)} cy={yKib(last.kib).toFixed(1)} r={3} fill={KIB} />
        {xticks.map((i, k) => (
          <text key={i} x={xAt(i)} y={H - 8} fontSize={10} fill={token.colorTextSecondary}
            textAnchor={k === 0 ? 'start' : k === xticks.length - 1 ? 'end' : 'middle'}>
            {fmtTime(points[i].t)}
          </text>
        ))}
      </svg>
      <Space size={12} wrap>
        <Typography.Text style={{ color: MSG }}>{`写入速度（数量）${fmtRate(last.msg)} msg/s`}</Typography.Text>
        <Typography.Text style={{ color: KIB }}>{`写入速度（大小）${fmtRate(last.kib)} KiB/s`}</Typography.Text>
      </Space>
    </Space>
  )
}

export default NodeRateChart
