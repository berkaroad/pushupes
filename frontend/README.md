# PushupES Console（前端管理台）

React 18 + Ant Design 5 + Vite 5 + TypeScript，支持 Light / Dark 主题切换（右上角开关，选择记忆在 localStorage）。

## 页面

- 集群 `/cluster`：节点、槽位数、迁移中槽位、Raft 状态统计与节点卡片（5s 自动刷新），「添加节点」弹窗经控制器把新节点加入 Raft 成员
- 槽位 `/slots`：128 槽分配表（状态/Leader 过滤），槽详情抽屉（HW/LastSeq/ISR/segments），发起迁移
- 事件流 `/streams`：追加事件（校验 version/command_id/幂等返回与 1003 MOVED 提示）、按聚合读取事件流

## 开发

```bash
npm install            # 沙箱内如 ~/.npm 有 root 残留用 --cache <dir>
npm run dev            # http://127.0.0.1:5173
```

dev server 将 `/api/*` 代理到后端节点（默认 `http://127.0.0.1:8091`，可用环境变量 `PUSHUPES_API` 覆盖，如多节点时指向 Raft leader）。

## API 地址配置与自动跟随 Leader

前端支持配置**多个 admin 地址**（英文逗号分隔），启动后自动探测并跟随 Raft leader——所有请求（状态轮询、每节点计数器、槽位详情、迁移等控制命令）都直接打到 leader 节点；leader 切换后下一次轮询自动重定向到新 leader，pinned leader 宕机时回退逐个探测其余地址。

配置方式（优先级从高到低）：

1. `index.html` 里的 `window.__PUSHUPES_ADMIN__`（运行时生效，改完不用重新构建）：

   ```html
   <script>window.__PUSHUPES_ADMIN__ = 'http://10.0.0.1:8091,http://10.0.0.2:8091,http://10.0.0.3:8091'</script>
   ```

   注意：该变量**留空即视为未配置**，会继续往下走环境变量。index.html 默认就是空串，所以 `npm run dev` / 默认构建无需改动该文件。

2. 构建期环境变量 `PUSHUPES_ADMIN_ENDPOINTS`（逗号分隔）：

   ```bash
   PUSHUPES_ADMIN_ENDPOINTS="http://10.0.0.1:8091,http://10.0.0.2:8091" npm run build
   ```

3. 都不配时退回 `/api`（即 dev server 单节点代理，行为同旧版）。

裸 `host:port` 也可写，前端自动补 `http://`。多地址模式下各节点 admin 面需允许 CORS（当前后端已放开）。

验证：`node scripts/leader-follow-check.mjs` —— 起 3 个假 admin 节点，用 esbuild 编译真实 `src/api.ts` 后断言：启动探测 → 钉住 leader → leader 切换后重钉 → 控制命令重试到新控制器 → leader 宕机回退探测地址池 → 全部宕机报可读错误。

`node scripts/live-leader-follow-check.mjs` —— 对 `.cluster` 真实三节点集群（无 HTTP 代理、直连各节点 admin 口走 CORS）的在线验证：杀掉当前 Raft leader 进程 → 等存活节点选出新 leader → 前端下一次轮询按顺序逐个轮询地址池直到可访问、并重新钉住新 leader → 最后自动拉起被杀节点恢复集群。

`node scripts/dev-pool-check.mjs` —— 按用户的 `PUSHUPES_ADMIN_ENDPOINTS=... npm run dev` 场景走一遍 vite 真实转换管线（`import.meta.env` 注入）＋ index.html 的 `window.__PUSHUPES_ADMIN__ = ''`，断言地址池确实来自环境变量（而非退化成 `/api` 单点代理），并杀掉地址池第一个节点验证轮询仍在工作。

`node scripts/auto-refresh-check.mjs` —— 钉住「集群」页的自动刷新：起两个假 admin 节点，status 里带一个每次请求都变的计数器，断言每次轮询都**真的重新拉取**（不是复用缓存的 promise）且返回**新对象**。React 对引用相等的 state 更新会直接跳过，所以只验证「数据变了」不够，必须验证对象身份也变——否则页面会静默冻在启动那一刻的快照。

## 构建

```bash
npm run build          # tsc -b && vite build -> dist/
npm run preview
```

## 验证

```bash
node scripts/ssr-check.mjs   # vite SSR + react-dom/server 断言渲染与主题 token 差异（浏览器工具不可达内网地址时的替代）
```
