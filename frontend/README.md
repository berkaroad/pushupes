# PushupES Console（前端管理台）

React 18 + Ant Design 5 + Vite 5 + TypeScript，支持 Light / Dark 主题切换（右上角开关，选择记忆在 localStorage）。

## 页面

- 集群总览 `/cluster`：节点、槽位数、迁移中槽位、Raft 状态统计与节点卡片（5s 自动刷新）
- 槽位 `/slots`：128 槽分配表（状态/Leader 过滤），槽详情抽屉（HW/LastSeq/ISR/segments），发起迁移
- 事件流 `/streams`：追加事件（校验 version/command_id/幂等返回与 1003 MOVED 提示）、按聚合读取事件流

## 开发

```bash
npm install            # 沙箱内如 ~/.npm 有 root 残留用 --cache <dir>
npm run dev            # http://127.0.0.1:5173
```

dev server 将 `/api/*` 代理到后端节点（默认 `http://127.0.0.1:8091`，可用环境变量 `PUSHUPES_API` 覆盖，如多节点时指向 Raft leader）。

## 构建

```bash
npm run build          # tsc -b && vite build -> dist/
npm run preview
```

## 验证

```bash
node scripts/ssr-check.mjs   # vite SSR + react-dom/server 断言渲染与主题 token 差异（浏览器工具不可达内网地址时的替代）
```
