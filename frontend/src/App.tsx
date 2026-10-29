import { useEffect, useState } from 'react'
import { RailContext } from './rail'
import { Layout, Menu, Segmented } from 'antd'
import { SunOutlined, MoonOutlined, ClusterOutlined, AppstoreOutlined } from '@ant-design/icons'
import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { useThemeMode } from './theme'
import ClusterPage from './pages/ClusterPage'
import SlotsPage from './pages/SlotsPage'

const items = [
  { key: '/cluster', icon: <ClusterOutlined />, label: '集群' },
  { key: '/slots', icon: <AppstoreOutlined />, label: '槽位' },
]

export default function App() {
  const { mode, toggle } = useThemeMode()
  const loc = useLocation()
  // The brand mark follows the Sider width: the rail auto-collapses on a narrow
  // viewport (breakpoint="lg"), and the logo scales down with it instead of a
  // word spilling out of the 64px gutter. onCollapse only fires on the
  // breakpoint transition and the trigger click, so this state mirrors the rail
  // without making the Sider controlled.
  const [collapsed, setCollapsed] = useState(false)
  useEffect(() => {
    // Seed from the real width on mount: a session that starts narrow never
    // fires an onCollapse event, and the brand would stay long in a rail that
    // is already collapsed.
    const onResize = () => setCollapsed(window.innerWidth < 992)
    onResize()
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider breakpoint="lg" collapsedWidth={64} onCollapse={setCollapsed}
        style={{ background: mode === 'dark' ? '#141414' : '#fff', borderRight: '1px solid rgba(128,128,128,0.2)' }}>
        <div style={{ padding: collapsed ? '16px 14px' : '12px 12px' }}>
          {collapsed
            // The rail is 64px wide: show the mark alone. The SVG is one square
            // (cube above, wordmark below), so a fixed-height window crops the
            // wordmark instead of shrinking it into an illegible smear.
            ? (
              <span style={{ display: 'block', width: 32, height: 22, overflow: 'hidden', borderRadius: 4 }}>
                <img src="/logo.svg" alt="PushupES" style={{ width: 32, display: 'block' }} />
              </span>
            )
            : <img src="/logo.svg" alt="PushupES" style={{ width: 120, display: 'block', borderRadius: 6 }} />}
        </div>
        <Menu
          theme="light"
          mode="inline"
          style={{ background: 'transparent', borderInlineEnd: 'none' }}
          selectedKeys={[loc.pathname]}
          items={items}
          onClick={(e) => { window.location.hash = '#' + e.key }}
        />
      </Layout.Sider>
      <Layout>
        <Layout.Header style={{
          background: mode === 'dark' ? '#141414' : '#fff',
          display: 'flex', alignItems: 'center', justifyContent: 'flex-end',
          paddingRight: 24, borderBottom: '1px solid rgba(128,128,128,0.2)',
        }}>
          <Segmented
            value={mode}
            options={[
              { value: 'light', label: <span><SunOutlined /> Light</span> },
              { value: 'dark', label: <span><MoonOutlined /> Dark</span> },
            ]}
            onChange={(v) => { if (v !== mode) toggle() }}
          />
        </Layout.Header>
        <Layout.Content style={{ padding: 24 }}>
          <RailContext.Provider value={collapsed}>
            <Routes>
              <Route path="/" element={<Navigate to="/cluster" replace />} />
              <Route path="/cluster" element={<ClusterPage />} />
              <Route path="/slots" element={<SlotsPage />} />
            </Routes>
          </RailContext.Provider>
        </Layout.Content>
      </Layout>
    </Layout>
  )
}
