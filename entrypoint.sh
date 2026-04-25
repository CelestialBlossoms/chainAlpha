#!/bin/bash
set -e

# 配置 gmgn-cli
mkdir -p ~/.config/gmgn
echo "GMGN_API_KEY=$GMGN_API_KEY" > ~/.config/gmgn/.env



# 启动阿尔法机器人
echo "Starting Deep Alpha Pro Robot..."
python deep_alpha_pro.py
