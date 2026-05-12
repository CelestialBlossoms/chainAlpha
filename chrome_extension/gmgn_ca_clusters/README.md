# Chain Alpha GMGN CA Clusters

本目录是 Manifest V3 Chrome 插件，用于在 GMGN token 页面显示本地 Chain Alpha CA 分析结果。

## 使用

1. 启动本地接口：

   ```powershell
   uvicorn web_dashboard.app:app --host 127.0.0.1 --port 8000
   ```

2. Chrome 打开 `chrome://extensions`，开启开发者模式。
3. 选择“加载已解压的扩展程序”，选择本目录：

   ```text
   D:\github\chainAlpha\chrome_extension\gmgn_ca_clusters
   ```

4. 打开 GMGN token 页面，右上角会出现 `CA Clusters` 面板。

插件只调用 `http://127.0.0.1:8000/api/ca-clusters`，GMGN 查询仍由本地 Python 服务执行。

## 排查

- 修改插件文件后，需要回到 `chrome://extensions`，点击本插件卡片上的刷新按钮。
- 如果 GMGN 页面没有 `CA Clusters` 面板，说明插件没有注入到当前 Chrome Profile；重新加载本目录并刷新 GMGN 页面。
- 如果面板显示本地接口失败，先确认 `http://127.0.0.1:8000` 可以打开。
- 如果面板显示 `No GMGN holder data returned`，说明请求已经到了本地 API，但 GMGN 对这个 CA 没返回 holder 列表。
