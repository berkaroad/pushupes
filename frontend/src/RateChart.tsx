import { Space, Typography, theme } from 'antd'

// Points the drawer keeps (2s apart): two minutes of history.
export const RATE_POINTS = 60

export function fmtRate(v: number): string {
  if (v === 0) return '0'
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k`
  return v.toFixed(v >= 10 ? 0 : 1)
}

// fmtTime renders a sample's wall clock as HH:MM:SS (hand-rolled so the label
// does not depend on the host locale).
export function fmtTime(ms: number): string {
  const d = new Date(ms)
  const p = (n: number) => String(n).padStart(2, '0')
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

export type RatePoint = { t: number; v: number }

// CaptionLine keeps the label quiet and paints the value in the primary colour
// so the numbers are the eye-catcher (the axis labels already say what the
// units are).
function CaptionLine({ label, value, tail }: { label: string; value: string; tail?: string }) {
  const { token } = theme.useToken()
  return (
    <Typography.Text type="secondary">
      {label}
      <Typography.Text strong style={{ color: token.colorPrimary }}>{value}</Typography.Text>
      {tail ? ` ${tail}` : ''}
    </Typography.Text>
  )
}

const W = 460
const H = 150
const PAD = { left: 46, right: 12, top: 16, bottom: 22 }

// RateChart draws a write-rate history as a plain SVG polyline: the console
// carries no charting dependency, and a rate series needs a scale, a baseline
// and the peak — nothing more.
export function RateChart({ points }: { points: RatePoint[] }) {
  const { token } = theme.useToken()
  if (points.length < 2) {
    return (
      <Typography.Text type="secondary">
        采集中…（每 2s 采样一次该槽 leader 的 durable 计数，两个样本后成图）
      </Typography.Text>
    )
  }
  const plotW = W - PAD.left - PAD.right
  const plotH = H - PAD.top - PAD.bottom
  const peak = Math.max(1, ...points.map((p) => p.v))
  const stepX = plotW / (points.length - 1)
  const xAt = (i: number) => PAD.left + i * stepX
  const yAt = (v: number) => PAD.top + (1 - v / peak) * plotH
  const poly = points.map((p, i) => `${xAt(i).toFixed(1)},${yAt(p.v).toFixed(1)}`).join(' ')
  const last = points[points.length - 1]
  // Samples are evenly spaced (2s), so the plain mean is the window average.
  const avg = points.reduce((sum, p) => sum + p.v, 0) / points.length

  // y ticks: 0 / mid / peak (evts/sec); x ticks: first, middle and last sample time
  const yticks = [0, peak / 2, peak]
  const n = points.length - 1
  const xticks = [...new Set(n >= 30
    ? [0, Math.floor(n / 3), Math.floor((2 * n) / 3), n]
    : n >= 2 ? [0, Math.floor(n / 2), n] : [0, n])]

  return (
    <Space direction="vertical" size={4} style={{ width: '100%' }}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} role="img"
        aria-label="写入速率折线图（横轴时间点，纵轴 evts/sec）">
        {yticks.map((v) => (
          <g key={v}>
            <line x1={PAD.left} y1={yAt(v)} x2={W - PAD.right} y2={yAt(v)}
              stroke={token.colorBorderSecondary} strokeDasharray={v === 0 ? undefined : '3 3'} />
            <text x={PAD.left - 6} y={yAt(v) + 3.5} fontSize={10} textAnchor="end"
              fill={token.colorTextSecondary}>{fmtRate(v)}</text>
          </g>
        ))}
        <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={H - PAD.bottom} stroke={token.colorBorder} />
        <line x1={PAD.left} y1={H - PAD.bottom} x2={W - PAD.right} y2={H - PAD.bottom} stroke={token.colorBorder} />
        <text x={PAD.left} y={10} fontSize={10} fill={token.colorTextSecondary}>evts/sec</text>
        <polyline points={poly} fill="none" stroke={token.colorPrimary} strokeWidth={2} />
        <circle cx={xAt(points.length - 1).toFixed(1)} cy={yAt(last.v).toFixed(1)} r={3} fill={token.colorPrimary} />
        {xticks.map((i, k) => (
          <text key={i} x={xAt(i)} y={H - 8} fontSize={10} fill={token.colorTextSecondary}
            textAnchor={k === 0 ? 'start' : k === xticks.length - 1 ? 'end' : 'middle'}>
            {fmtTime(points[i].t)}
          </text>
        ))}
      </svg>
      <Space direction="vertical" size={0} style={{ width: '100%' }}>
        <Typography.Text type="secondary">横轴：时间点（每 2s 一个样本）</Typography.Text>
        <Typography.Text type="secondary">纵轴：evts/sec（写入速率）</Typography.Text>
        <CaptionLine label="最新 " value={fmtRate(last.v)} tail="evts/sec" />
        <CaptionLine label="峰值 " value={fmtRate(peak)} tail="evts/sec" />
        <CaptionLine label="平均 " value={fmtRate(avg)} tail="evts/sec" />
        <CaptionLine label="样本 " value={String(points.length)} tail={`（最近 ${RATE_POINTS} 个）`} />
      </Space>
    </Space>
  )
}

export default RateChart
