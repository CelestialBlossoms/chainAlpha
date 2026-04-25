FROM python:3.10-slim

# 安装 Node.js (gmgn-cli 的运行环境)
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    && curl -sL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# 全局安装 gmgn-cli
RUN npm install -g gmgn-cli

WORKDIR /app

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

# 给予入口脚本执行权限
RUN chmod +x entrypoint.sh

# 默认环境变量
ENV DATABASE_URL=postgresql://xf22610:1314zxcV1314@43.163.225.175:5432/chainAlpha

ENTRYPOINT ["./entrypoint.sh"]
