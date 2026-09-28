import { createContext, useContext, useMemo, useState } from 'react'
import { App, ConfigProvider, theme } from 'antd'
import zhCN from 'antd/locale/zh_CN'

type Mode = 'light' | 'dark'

const ThemeCtx = createContext<{ mode: Mode; toggle: () => void }>({
  mode: 'light',
  toggle: () => {},
})

export function useThemeMode() {
  return useContext(ThemeCtx)
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [mode, setMode] = useState<Mode>(
    () => (localStorage.getItem('pushupes-theme') as Mode) ?? 'light',
  )
  const value = useMemo(
    () => ({
      mode,
      toggle: () => {
        setMode((m) => {
          const next = m === 'light' ? 'dark' : 'light'
          localStorage.setItem('pushupes-theme', next)
          return next
        })
      },
    }),
    [mode],
  )
  return (
    <ThemeCtx.Provider value={value}>
      <ConfigProvider
        locale={zhCN}
        theme={{
          algorithm: mode === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm,
        }}
      >
        <App>{children}</App>
      </ConfigProvider>
    </ThemeCtx.Provider>
  )
}
