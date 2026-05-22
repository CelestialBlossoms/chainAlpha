# Chain Alpha Project Setup and Deployment

This document has two versions:

- [涓枃鐗堟湰](#涓枃鐗堟湰)
- [English Version](#english-version)

## 涓枃鐗堟湰

鏈枃妗ｈ鏄?Chain Alpha 鐨勫垵濮嬪寲銆侀厤缃€佽繍琛屻€丏ocker 閮ㄧ讲鍜?Chrome 鎻掍欢浣跨敤鏂瑰紡銆?
### 1. 鐜鍑嗗

寤鸿浣跨敤 Python 3.12銆?
瀹夎 Python 渚濊禆锛?
```powershell
D:\software\anaconda\envs\py312\python.exe -m pip install -r requirements.txt
```

Linux 绀轰緥锛?
```bash
python3 -m pip install -r requirements.txt
```

鏈湴杩愯闇€瑕佸畨瑁?`gmgn-cli`锛?
```bash
npm install -g gmgn-cli
gmgn-cli --help
```

Docker 闀滃儚浼氳嚜鍔ㄥ畨瑁?Node.js 鍜?`gmgn-cli`銆?
### 2. 椤圭洰閰嶇疆

鏁忔劅閰嶇疆涓嶈鍐欏叆浠ｇ爜锛屼篃涓嶈鎻愪氦鍒?Git銆傞」鐩細鑷姩璇诲彇锛?
- `.env`
- `gmgn_account_2/.env`

鍒濆鍖栭厤缃枃浠讹細

```powershell
Copy-Item .env.example .env
New-Item -ItemType Directory -Force gmgn_account_2
New-Item -ItemType File -Force gmgn_account_2\.env
```

`.env` 绀轰緥锛?
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

`gmgn_account_2/.env` 绀轰緥锛?
```dotenv
GMGN_API_KEY=your_gmgn_api_key
```

### 3. 鏁版嵁搴撳垵濮嬪寲

椤圭洰浣跨敤 PostgreSQL銆傚厛纭 `.env` 涓殑 `DATABASE_URL` 鍙互杩炴帴銆?
鍒濆鍖栧熀纭€琛細

```powershell
D:\software\anaconda\envs\py312\python.exe init_db.py
```

鍒濆鍖栧簳閮ㄧ洃鎺?watchlist 琛細

```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\init_bottom_watchlist_db.py
```

鍒濆鍖栧簳閮?Top100 蹇収鍜?K 绾跨紦瀛樿〃锛?
```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\init_bottom_accumulation_db.py
```

娉ㄦ剰锛歚bottom_detection/init_bottom_accumulation_db.py` 浼氬垹闄ゅ苟閲嶅缓鐩稿叧蹇収琛ㄣ€傜敓浜х幆澧冩墽琛屽墠瑕佺‘璁よ繖浜涚紦瀛樺拰蹇収鏁版嵁鍙互娓呯┖銆?
### 4. Redis 閰嶇疆

Redis 鐢ㄤ簬 Telegram 鍛婅娴併€佹彃浠朵俊鍙锋祦鍜岄儴鍒嗙紦瀛樸€?
```dotenv
CHAIN_ALPHA_REDIS_HOST=localhost
CHAIN_ALPHA_REDIS_PORT=6379
CHAIN_ALPHA_REDIS_PASSWORD=
CHAIN_ALPHA_REDIS_DB=0
CHAIN_ALPHA_REDIS_ENABLED=1
```

濡傛灉涓存椂涓嶄娇鐢?Redis锛?
```dotenv
CHAIN_ALPHA_REDIS_ENABLED=0
```

### 5. Telegram 閰嶇疆涓庤繍琛?
Telegram Bot 闇€瑕侀厤缃細

```dotenv
TG_BOT_TOKEN=your_bot_token
TG_CHAT_ID=your_chat_id
```

鍚姩 CA 绛圭爜鍒嗘瀽 Telegram Bot锛?
```powershell
D:\software\anaconda\envs\py312\python.exe tg_ca_chip_alert_bot.py
```

鍚姩搴曢儴 Top100 鐩戞帶骞跺彂閫?Telegram 閫氱煡锛?
```powershell
D:\software\anaconda\envs\py312\python.exe bottom_detection\bottom_accumulation_monitor.py --watch --notify
```

鍚姩 Deep Alpha 涓昏繘绋嬶細

```powershell
D:\software\anaconda\envs\py312\python.exe -m deep_alpha.deep_alpha_pro
```

鍚姩 Web Dashboard锛?
```powershell
D:\software\anaconda\envs\py312\python.exe -m uvicorn web_dashboard.app:app --host 0.0.0.0 --port 8089
```

璁块棶鍦板潃锛?
```text
http://127.0.0.1:8089
```

### 6. Docker 閮ㄧ讲

鏋勫缓闀滃儚锛?
```bash
docker compose build
```

鍚姩 Deep Alpha 涓绘湇鍔★細

```bash
docker compose up -d
```

鍚姩 Web Dashboard锛?
```bash
docker compose -f docker-compose.dashboard.yml up -d --build
```

榛樿绔彛锛?
```text
Host 8010 -> Container 8089
```

鍚姩 Telegram CA Bot锛?
```bash
docker compose -f docker-compose.tg-ca.yml up -d --build
```

鍚姩搴曢儴 Top100 鐩戞帶锛?
```bash
docker compose -f docker-compose.bottom-top100.yml up -d --build
```

鍚姩 CA Clusters API锛?
```bash
```

榛樿绔彛锛?
```text
Host 8012 -> Container 8089
```

鏌ョ湅瀹瑰櫒鐘舵€侊細

```bash
docker ps --filter name=chain-alpha
```

鏌ョ湅鏃ュ織锛?
```bash
docker logs -f chain-alpha-robot
docker logs -f chain-alpha-tg-dashboard
docker logs -f chain-alpha-tg-ca-bot
docker logs -f chain-alpha-bottom-top100
```

瀹瑰櫒浼氭寕杞藉綋鍓嶉」鐩洰褰曞埌 `/app`锛屽洜姝や細璇诲彇椤圭洰鏍圭洰褰曠殑 `.env` 鍜?`gmgn_account_2/.env`銆?
### 7. Chrome 鎻掍欢浣跨敤

鎻掍欢鐩綍锛?
```text
chrome_extension/gmgn_ca_clusters
```

鐢ㄩ€旓細鍦?GMGN token 椤甸潰灞曠ず Chain Alpha 鐨?CA 绛圭爜鍜岄泦缇ゅ垎鏋愰潰鏉裤€?
鏈湴璋冭瘯鍚庣锛?
```powershell
D:\software\anaconda\envs\py312\python.exe -m uvicorn web_dashboard.app:app --host 127.0.0.1 --port 8000
```

褰撳墠鎻掍欢 `background.js` 榛樿璇锋眰锛?
```text
http://<server-host>:8010
```

濡傞渶鏀规垚鏈湴璋冭瘯锛屼慨鏀?`chrome_extension/gmgn_ca_clusters/background.js`锛?
```js
const SERVICE_URLS = {
  server: "http://127.0.0.1:8000",
};
```

鍔犺浇鎻掍欢锛?
1. 鎵撳紑 Chrome銆?2. 璁块棶 `chrome://extensions`銆?3. 鎵撳紑鈥滃紑鍙戣€呮ā寮忊€濄€?4. 鐐瑰嚮鈥滃姞杞藉凡瑙ｅ帇鐨勬墿灞曠▼搴忊€濄€?5. 閫夋嫨 `D:\github\chainAlpha\chrome_extension\gmgn_ca_clusters`銆?6. 鎵撳紑 GMGN token 椤甸潰锛屽彸涓婅浼氬嚭鐜?`CA Clusters` 闈㈡澘銆?
淇敼鎻掍欢浠ｇ爜鍚庯紝闇€瑕佸湪 `chrome://extensions` 鍒锋柊鎻掍欢锛岀劧鍚庡埛鏂?GMGN 椤甸潰銆?
### 8. 甯歌妫€鏌?
妫€鏌ョ幆澧冨彉閲忔槸鍚﹁璇诲彇锛?
```powershell
D:\software\anaconda\envs\py312\python.exe -c "import config, redis_client; print(config.DB_CONFIG['host']); print(redis_client.REDIS_HOST); print(bool(config.GMGN_API_KEY))"
```

妫€鏌?Dashboard锛?
```bash
curl http://127.0.0.1:8089/
```

妫€鏌ユ晱鎰熸枃浠舵槸鍚﹁ Git 蹇界暐锛?
```bash
git check-ignore -v .env gmgn_account_2/.env gmgn_account_2/gmgn_private_2.pem
```

鎻愪氦鍓嶆壂鎻忔晱鎰熷€硷細

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
D:\software\anaconda\envs\py312\python.exe -m deep_alpha.deep_alpha_pro
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
4. Click 鈥淟oad unpacked鈥?
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

