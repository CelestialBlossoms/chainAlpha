# Linux Bottom Top100 更新操作

本文件记录从 Windows 本地通过 Python SSH 登录 Linux 服务器，更新 `/opt/chainAlpha` 并重启 `docker-compose.bottom-top100.yml` 的操作流程。

## 适用场景

- 服务器用户：`root`
- 项目目录：`/opt/chainAlpha`
- 更新服务：`docker-compose.bottom-top100.yml`
- 执行动作：`git pull` 后重建并启动 `bottom-top100-monitor`
- 当前检测范围：只读取数据库 `bottom_watchlist_tokens` 里的 CA 做底部异动/EMA 检测，不再直接扫描 GMGN 热门榜 token。

## 前置要求

本地 Python 环境需要安装 `paramiko`：

```powershell
D:\software\anaconda\envs\py312\python.exe -m pip install paramiko
```

## 执行更新

在项目根目录 `D:\github\chainAlpha` 执行：

```powershell
@'
import paramiko
import sys

host = "43.163.225.175"
username = "root"
password = "1314zxcV1314"

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

host = "43.163.225.175"
username = "root"
password = "1314zxcV1314"
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
