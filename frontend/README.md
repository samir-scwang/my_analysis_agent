# Analysis Agent Frontend

这是一个面向最终用户的单屏 `React + Vite` 应用，界面结构参考 ChatGPT / Codex 的会话线程模式。

## 当前结构

- 左侧会话线程侧栏：支持新建、切换、重命名、删除和复制会话链接
- 右侧当前会话工作区：编辑标题和任务说明，上传该会话对应的文件夹
- 会话内展示：执行进度、阶段状态、上传目录、最终报告、图表、表格
- 所有产物都支持直接下载
- 单屏内容区：桌面端不依赖整页上下滚动，滚动被收在卡片内部
- 对接 FastAPI：创建会话、上传整个文件夹、查看会话产物和下载文件

## 前端启动

```bash
cd frontend
npm install
npm run dev
```

默认会请求：

```bash
http://localhost:8000
```

如需修改，可在 `.env` 中设置：

```bash
VITE_API_BASE_URL=http://localhost:8000
```

## FastAPI 启动

在项目根目录执行：

```bash
pip install -r requirements.txt
uvicorn app.api_frontend:app --reload
```

## FastAPI 已提供的接口

- `GET /health`
- `GET /api/frontend/workspace`
- `GET /api/frontend/sessions`
- `GET /api/frontend/sessions/{session_id}`
- `POST /api/frontend/sessions`
- `PATCH /api/frontend/sessions/{session_id}`
- `DELETE /api/frontend/sessions/{session_id}`
- `POST /api/frontend/sessions/{session_id}/folder`
- `GET /api/frontend/files/...`

当前接口使用项目本地 JSON 做轻量持久化，会话文件和产物会存放在：

```bash
app/artifacts/frontend_state/workspace_store.json
app/artifacts/frontend_state/sessions/<session_id>/
```

每个会话目录下会包含：

- `uploads/`：原始上传文件夹内容
- `generated/charts/`：图表文件
- `generated/tables/`：表格文件
- `generated/reports/`：最终报告

如果你想清空旧的演示会话并重新看一版干净界面，可以删除 `app/artifacts/frontend_state/workspace_store.json` 和 `app/artifacts/frontend_state/sessions/` 后重启 FastAPI，系统会重新注入一份新的示例状态。

后续如果要接真实工作流，可以把 `POST /api/frontend/sessions/{session_id}/folder` 里的示例产物复制逻辑替换成实际的 graph 调用或后台任务。
