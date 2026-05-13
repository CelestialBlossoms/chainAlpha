# Chain Alpha Project Setup and Deployment

This document has two versions:

- [中文版本](#中文版本)
- [English Version](#english-version)

## 中文版本

本文档说明 Chain Alpha 的初始化、配置、运行、Docker 部署和 Chrome 插件使用方式。

### 1. 环境准备

建议使用 Python 3.12。

安装 Python 依赖：

```powershell
D:\software\anaconda\envs\py312\python.exe -m pip install -r requirements.txt
```

Linux 示例：

```bash
python3 -m pip install -r requirements.txt
```

本地运行需要安装 `gmgn-cli`：

```bash
npm install -g gmgn-cli
gmgn-cli --help
```

Docker 镜像会自动安装 Node.js 和 `gmgn-cli`。

### 2. 项目配置

敏感配置不要写入代码，也不要提交到 Git。项目会自动读取：

- `.env`
- `gmgn_account_2/.env`

初始化配置文件：

```powershell
Copy-Item .env.example .env
New-Item -ItemType Directory -Force gmgn_account_2
New-Item -ItemType File -Force gmgn_account_2\.env
```

`.env` 示例：

```dotenv
DATABASE_URL=postgresql://user:password@host:5432/chainAlpha

CHAIN_ALPHA_REDIS_HOST=localhost
CHAIN_ALPHA_REDIS_PORT=6379
CHAIN_ALPHA_REDIS_PASSWORD=
CHAIN_ALPHA_REDIS_DB=0
CHAIN_ALPHA_REDIS_ENABLED=1

TG_BOT_TOKEN=
TG_CHAT_ID=
DEEPSEEK_API_KEY=
GMGN_CLI_ENV_FILE=gmgn_account_2/.env
```

`gmgn_account_2/.env` 示例：

```dotenv
GMGN_API_KEY=your_gmgn_api_key
```

### 3. 数据库初始化

项目使用 PostgreSQL。先确认 `.env` 中的 `DATABASE_URL` 可以连接。

初始化基础表：

```powershell
D:\software\anaconda\envs\py312\python.exe init_db.py
```

初始化底部监控 watchlist 表：

```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\init_bottom_watchlist_db.py
```

初始化底部 Top100 快照和 K 线缓存表：

```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\init_bottom_accumulation_db.py
```

注意：`bottom_detection/init_bottom_accumulation_db.py` 会删除并重建相关快照表。生产环境执行前要确认这些缓存和快照数据可以清空。

### 4. Redis 配置

Redis 用于 Telegram 告警流、插件信号流和部分缓存。

```dotenv
CHAIN_ALPHA_REDIS_HOST=localhost
CHAIN_ALPHA_REDIS_PORT=6379
CHAIN_ALPHA_REDIS_PASSWORD=
CHAIN_ALPHA_REDIS_DB=0
CHAIN_ALPHA_REDIS_ENABLED=1
```

如果临时不使用 Redis：

```dotenv
CHAIN_ALPHA_REDIS_ENABLED=0
```

### 5. Telegram 配置与运行

Telegram Bot 需要配置：

```dotenv
TG_BOT_TOKEN=your_bot_token
TG_CHAT_ID=your_chat_id
```

启动 CA 筹码分析 Telegram Bot：

```powershell
D:\software\anaconda\envs\py312\python.exe tg_ca_chip_alert_bot.py
```

启动底部 Top100 监控并发送 Telegram 通知：

```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\bottom_accumulation_monitor.py --watch --notify
```

启动 Deep Alpha 主进程：

```powershell
D:\software\anaconda\envs\py312\python.exe deep_alpha_pro.py
```

启动 Web Dashboard：

```powershell
D:\software\anaconda\envs\py312\python.exe -m uvicorn web_dashboard.app:app --host 0.0.0.0 --port 8089
```

访问地址：

```text
http://127.0.0.1:8089
```

### 6. Docker 部署

构建镜像：

```bash
docker compose build
```

启动 Deep Alpha 主服务：

```bash
docker compose up -d
```

启动 Web Dashboard：

```bash
docker compose -f docker-compose.dashboard.yml up -d --build
```

默认端口：

```text
Host 8010 -> Container 8089
```

启动 Telegram CA Bot：

```bash
docker compose -f docker-compose.tg-ca.yml up -d --build
```

启动底部 Top100 监控：

```bash
docker compose -f docker-compose.bottom-top100.yml up -d --build
```

启动 CA Clusters API：

```bash
docker compose -f docker-compose.ca-clusters.yml up -d --build
```

默认端口：

```text
Host 8012 -> Container 8089
```

查看容器状态：

```bash
docker ps --filter name=chain-alpha
```

查看日志：

```bash
docker logs -f chain-alpha-robot
docker logs -f chain-alpha-tg-dashboard
docker logs -f chain-alpha-tg-ca-bot
docker logs -f chain-alpha-bottom-top100
docker logs -f chain-alpha-ca-clusters-api
```

容器会挂载当前项目目录到 `/app`，因此会读取项目根目录的 `.env` 和 `gmgn_account_2/.env`。

### 7. Chrome 插件使用

插件目录：

```text
chrome_extension/gmgn_ca_clusters
```

用途：在 GMGN token 页面展示 Chain Alpha 的 CA 筹码和集群分析面板。

本地调试后端：

```powershell
D:\software\anaconda\envs\py312\python.exe -m uvicorn web_dashboard.app:app --host 127.0.0.1 --port 8000
```

当前插件 `background.js` 默认请求：

```text
http://<server-host>:8010
```

如需改成本地调试，修改 `chrome_extension/gmgn_ca_clusters/background.js`：

```js
const SERVICE_URLS = {
  server: "http://127.0.0.1:8000",
};
```

加载插件：

1. 打开 Chrome。
2. 访问 `chrome://extensions`。
3. 打开“开发者模式”。
4. 点击“加载已解压的扩展程序”。
5. 选择 `D:\github\chainAlpha\chrome_extension\gmgn_ca_clusters`。
6. 打开 GMGN token 页面，右上角会出现 `CA Clusters` 面板。

修改插件代码后，需要在 `chrome://extensions` 刷新插件，然后刷新 GMGN 页面。

### 8. 常见检查

检查环境变量是否被读取：

```powershell
D:\software\anaconda\envs\py312\python.exe -c "import config, redis_client; print(config.DB_CONFIG['host']); print(redis_client.REDIS_HOST); print(bool(config.GMGN_API_KEY))"
```

检查 Dashboard：

```bash
curl http://127.0.0.1:8089/
```

检查敏感文件是否被 Git 忽略：

```bash
git check-ignore -v .env gmgn_account_2/.env gmgn_account_2/gmgn_private_2.pem
```

提交前扫描敏感值：

```bash
git grep -n -E "postgresql://[^ ]*@|GMGN_API_KEY=|TG_BOT_TOKEN=|sk-[A-Za-z0-9]{20,}"
```

## English Version

This document explains how to initialize, configure, run, deploy, and use the Chrome extension for Chain Alpha.

### 1. Environment

Python 3.12 is recommended.

Install Python dependencies:

```powershell
D:\software\anaconda\envs\py312\python.exe -m pip install -r requirements.txt
```

Linux example:

```bash
python3 -m pip install -r requirements.txt
```

Local runs require `gmgn-cli`:

```bash
npm install -g gmgn-cli
gmgn-cli --help
```

The Docker image installs Node.js and `gmgn-cli` automatically.

### 2. Configuration

Do not hardcode secrets in source code and do not commit them to Git. The project automatically loads:

- `.env`
- `gmgn_account_2/.env`

Create local config files:

```powershell
Copy-Item .env.example .env
New-Item -ItemType Directory -Force gmgn_account_2
New-Item -ItemType File -Force gmgn_account_2\.env
```

Example `.env`:

```dotenv
DATABASE_URL=postgresql://user:password@host:5432/chainAlpha

CHAIN_ALPHA_REDIS_HOST=localhost
CHAIN_ALPHA_REDIS_PORT=6379
CHAIN_ALPHA_REDIS_PASSWORD=
CHAIN_ALPHA_REDIS_DB=0
CHAIN_ALPHA_REDIS_ENABLED=1

TG_BOT_TOKEN=
TG_CHAT_ID=
DEEPSEEK_API_KEY=
GMGN_CLI_ENV_FILE=gmgn_account_2/.env
```

Example `gmgn_account_2/.env`:

```dotenv
GMGN_API_KEY=your_gmgn_api_key
```

### 3. Database Initialization

The project uses PostgreSQL. Confirm that `DATABASE_URL` in `.env` is reachable before running initialization scripts.

Initialize base tables:

```powershell
D:\software\anaconda\envs\py312\python.exe init_db.py
```

Initialize the bottom monitor watchlist table:

```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\init_bottom_watchlist_db.py
```

Initialize bottom Top100 snapshots and K-line cache tables:

```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\init_bottom_accumulation_db.py
```

Warning: `bottom_detection/init_bottom_accumulation_db.py` drops and recreates the related snapshot tables. Confirm this is acceptable before running it in production.

### 4. Redis Configuration

Redis is used for Telegram alert streams, plugin signal streams, and some cache data.

```dotenv
CHAIN_ALPHA_REDIS_HOST=localhost
CHAIN_ALPHA_REDIS_PORT=6379
CHAIN_ALPHA_REDIS_PASSWORD=
CHAIN_ALPHA_REDIS_DB=0
CHAIN_ALPHA_REDIS_ENABLED=1
```

To disable Redis temporarily:

```dotenv
CHAIN_ALPHA_REDIS_ENABLED=0
```

### 5. Telegram Configuration and Runtime

Configure the Telegram bot:

```dotenv
TG_BOT_TOKEN=your_bot_token
TG_CHAT_ID=your_chat_id
```

Start the CA chip analysis Telegram bot:

```powershell
D:\software\anaconda\envs\py312\python.exe tg_ca_chip_alert_bot.py
```

Start the bottom Top100 monitor with Telegram notifications:

```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\bottom_accumulation_monitor.py --watch --notify
```

Start the Deep Alpha main process:

```powershell
D:\software\anaconda\envs\py312\python.exe deep_alpha_pro.py
```

Start the Web Dashboard:

```powershell
D:\software\anaconda\envs\py312\python.exe -m uvicorn web_dashboard.app:app --host 0.0.0.0 --port 8089
```

Open:

```text
http://127.0.0.1:8089
```

### 6. Docker Deployment

Build the image:

```bash
docker compose build
```

Start the Deep Alpha main service:

```bash
docker compose up -d
```

Start the Web Dashboard:

```bash
docker compose -f docker-compose.dashboard.yml up -d --build
```

Default port mapping:

```text
Host 8010 -> Container 8089
```

Start the Telegram CA bot:

```bash
docker compose -f docker-compose.tg-ca.yml up -d --build
```

Start the bottom Top100 monitor:

```bash
docker compose -f docker-compose.bottom-top100.yml up -d --build
```

Start the CA Clusters API:

```bash
docker compose -f docker-compose.ca-clusters.yml up -d --build
```

Default port mapping:

```text
Host 8012 -> Container 8089
```

Check containers:

```bash
docker ps --filter name=chain-alpha
```

Follow logs:

```bash
docker logs -f chain-alpha-robot
docker logs -f chain-alpha-tg-dashboard
docker logs -f chain-alpha-tg-ca-bot
docker logs -f chain-alpha-bottom-top100
docker logs -f chain-alpha-ca-clusters-api
```

Containers mount the current project directory to `/app`, so they read `.env` and `gmgn_account_2/.env` from the project root.

### 7. Chrome Extension Usage

Extension directory:

```text
chrome_extension/gmgn_ca_clusters
```

Purpose: show Chain Alpha CA chip and cluster analysis on GMGN token pages.

Start a local backend for debugging:

```powershell
D:\software\anaconda\envs\py312\python.exe -m uvicorn web_dashboard.app:app --host 127.0.0.1 --port 8000
```

The current `background.js` defaults to:

```text
http://<server-host>:8010
```

For local debugging, edit `chrome_extension/gmgn_ca_clusters/background.js`:

```js
const SERVICE_URLS = {
  server: "http://127.0.0.1:8000",
};
```

Load the extension:

1. Open Chrome.
2. Go to `chrome://extensions`.
3. Enable Developer mode.
4. Click “Load unpacked”.
5. Select `D:\github\chainAlpha\chrome_extension\gmgn_ca_clusters`.
6. Open a GMGN token page. The `CA Clusters` panel appears in the upper-right corner.

After changing extension files, refresh the extension on `chrome://extensions`, then refresh the GMGN page.

### 8. Common Checks

Check whether config values are loaded:

```powershell
D:\software\anaconda\envs\py312\python.exe -c "import config, redis_client; print(config.DB_CONFIG['host']); print(redis_client.REDIS_HOST); print(bool(config.GMGN_API_KEY))"
```

Check the Dashboard:

```bash
curl http://127.0.0.1:8089/
```

Check whether secret files are ignored by Git:

```bash
git check-ignore -v .env gmgn_account_2/.env gmgn_account_2/gmgn_private_2.pem
```

Scan for sensitive values before committing:

```bash
git grep -n -E "postgresql://[^ ]*@|GMGN_API_KEY=|TG_BOT_TOKEN=|sk-[A-Za-z0-9]{20,}"
```
