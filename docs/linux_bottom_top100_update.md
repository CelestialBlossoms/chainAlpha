# Linux Bottom Top100 更新操作

本文件记录从 Windows 本地通过 Python SSH 登录 Linux 服务器，更新 `/opt/chainAlpha` 并重启 `docker-compose.bottom-top100.yml` 的操作流程。

## 适用场景

- 服务器用户：`root`
- 项目目录：`/opt/chainAlpha`
- 更新服务：`docker-compose.bottom-top100.yml`
- 执行动作：`git pull` 后重建并启动 `bottom-top100-monitor`
- 当前检测范围：只读取数据库 `bottom_watchlist_tokens` 里的 CA 做底部异动/EMA 检测，不再直接扫描 GMGN 热门榜 token。

## 前置要求

### 方式一：使用本机 SSH 配置（推荐）

如果本机已经配置好 `~/.ssh/config`：

```sshconfig
Host chainalpha-server
    HostName 43.163.225.175
    User root
    IdentityFile ~/.ssh/dongj2.pem
    PubkeyAuthentication yes
    PreferredAuthentications publickey
```

可以直接通过 `chainalpha-server` 登录 Linux 服务器执行更新，不需要在命令里写服务器密码。

### 方式二：使用 Python Paramiko

本地 Python 环境需要安装 `paramiko`：

```powershell
D:\software\anaconda\envs\py312\python.exe -m pip install paramiko
```

## 执行更新

### 推荐命令：SSH 拉代码并更新底部异动服务

在 Windows 本地项目根目录 `D:\github\chainAlpha` 执行：

```powershell
ssh chainalpha-server "cd /opt/chainAlpha && git pull --ff-only && docker compose -f docker-compose.bottom-top100.yml up -d --build"
```

执行完成后检查容器状态：

```powershell
ssh chainalpha-server "docker ps --filter name=chain-alpha-bottom-top100 --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'"
```

查看最近日志：

```powershell
ssh chainalpha-server "docker logs --tail 80 chain-alpha-bottom-top100 2>&1"
```

确认远端代码版本和工作区状态：

```powershell
ssh chainalpha-server "cd /opt/chainAlpha && git rev-parse --short HEAD && git status --short --branch"
```

本地仓库也需要同步时，先在 `D:\github\chainAlpha` 执行：

```powershell
git pull --ff-only
```

### 备用命令：Paramiko 登录执行

在项目根目录 `D:\github\chainAlpha` 执行：

```powershell
@'
import paramiko
import sys

host = "<server-host>"
username = "root"
password = "<server-password>"

commands = [
    "cd /opt/chainAlpha && pwd && git rev-parse --short HEAD",
    "cd /opt/chainAlpha && git pull",
    "cd /opt/chainAlpha && docker compose -f docker-compose.bottom-top100.yml up -d --build",
]

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    client.connect(
        hostname=host,
        username=username,
        password=password,
        timeout=20,
        banner_timeout=20,
        auth_timeout=20,
    )

    final_code = 0
    for command in commands:
        print(f"\n$ {command}")
        stdin, stdout, stderr = client.exec_command(command, timeout=900)
        code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")

        if out:
            print(out, end="")
        if err:
            print(err, end="", file=sys.stderr)

        print(f"[exit] {code}")
        if code != 0:
            final_code = code
            break

    raise SystemExit(final_code)
finally:
    client.close()
'@ | D:\software\anaconda\envs\py312\python.exe -
```

## 成功标准

看到类似输出即表示更新成功：

```text
/opt/chainAlpha
[exit] 0

Already up to date.
[exit] 0

Service bottom-top100-monitor  Built
Container chain-alpha-bottom-top100  Started
[exit] 0
```

## 常见提示

如果出现下面提示，一般不是错误：

```text
Found orphan containers ([chain-alpha-tg-ca-bot chain-alpha-tg-dashboard])
```

原因是本次只使用 `docker-compose.bottom-top100.yml` 更新底部监控服务，Docker 发现同项目下还有其他 compose 服务不在当前文件中。只要 `chain-alpha-bottom-top100` 显示 `Started` 即可。

## 只查看远端状态

需要检查容器状态时执行：

```powershell
@'
import paramiko

host = "<server-host>"
username = "root"
password = "<server-password>"
command = "docker ps --filter name=chain-alpha-bottom-top100 --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'"

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=host, username=username, password=password, timeout=20)
stdin, stdout, stderr = client.exec_command(command, timeout=60)
print(stdout.read().decode("utf-8", errors="replace"))
err = stderr.read().decode("utf-8", errors="replace")
if err:
    print(err)
client.close()
'@ | D:\software\anaconda\envs\py312\python.exe -
```
