import { Layout, Menu, Segmented, Typography } from 'antd'
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
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider breakpoint="lg" collapsedWidth={64}
        style={{ background: mode === 'dark' ? '#141414' : '#fff', borderRight: '1px solid rgba(128,128,128,0.2)' }}>
        <div style={{ padding: '16px 12px' }}>
          <Typography.Text strong style={{ fontSize: 16 }}>
            PushupES
          </Typography.Text>
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
          <Routes>
            <Route path="/" element={<Navigate to="/cluster" replace />} />
            <Route path="/cluster" element={<ClusterPage />} />
            <Route path="/slots" element={<SlotsPage />} />
          </Routes>
        </Layout.Content>
      </Layout>
    </Layout>
  )
}
