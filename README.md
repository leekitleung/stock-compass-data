# stock-compass-data

## 自动获取 Tushare 数据

仓库包含 GitHub Actions 定时任务和本地脚本，用于通过代理拉取 Tushare 数据。

- 代理地址：`http://47.109.97.125:8080/tushare`
- 默认抓取：`stock_basic`（list_status=L）、`trade_cal`（exchange=SSE）
- 输出：`data/<api_name>/<api_name>_YYYYMMDD.parquet` 以及 `latest.parquet`

### 本地运行
```powershell
$env:TUSHARE_TOKEN = "<your_token>"
$env:TUSHARE_PROXY = "http://47.109.97.125:8080/tushare"
python scripts/fetch_tushare.py
```
- 若需 CSV：`$env:TUSHARE_FORMAT = "csv"`
- 自定义接口列表：设置 `TUSHARE_APIS` 为 JSON 数组，示例见 `scripts/fetch_tushare.py`。

### GitHub Actions
- Workflow: `.github/workflows/fetch_tushare.yml`
- Secrets: `TUSHARE_TOKEN`
- 可选变量: `PUSH_DATA=true` 开启后会自动 commit/push `data/`
- 定时：每天 08:30（北京时间），亦可手动触发

### 依赖
- `requests`, `pandas`, `pyarrow`
