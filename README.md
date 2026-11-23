# 虫巢工坊 · 分布式爬虫调度与反爬对抗平台

面向大规模网页采集的分布式爬虫调度、代理池管理、反爬策略对抗、内容抽取清洗与采集结果交付平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集站点 | `crawl_site` | 目标站点 | 站点编号、站点域名、站点类型 |
| 代理池 | `proxy_pool` | 代理节点 | 节点编号、代理地址、代理类型 |
| 反爬对抗 | `anti_block` | 对抗策略 | 策略编号、目标站点、对抗类型 |
| 采集任务 | `crawl_job` | 采集作业 | 作业编号、目标站点、入口URL |
| URL去重 | `url_dedup` | 去重指纹 | 指纹编号、源URL、指纹算法 |
| 内容抽取 | `content_extract` | 抽取规则 | 规则编号、目标站点、XPath选择器 |
| 字段清洗 | `field_clean` | 清洗规则 | 规则编号、目标字段、清洗类型 |
| 批次存储 | `batch_store` | 采集批次 | 批次编号、来源作业、数据行数 |
| 增量采集 | `incr_crawl` | 增量标记 | 标记编号、目标站点、增量字段 |
| 解析日志 | `parse_log` | 解析记录 | 记录编号、来源URL、解析规则 |
| 数据投递 | `data_delivery` | 投递任务 | 任务编号、数据来源、目标系统 |
| 质量抽样 | `quality_sample` | 抽样计划 | 计划编号、目标批次、抽样比例 || 站点监控 | `site_monitor` | 监控指标 | 指标编号、目标站点、监控类型 |
| 验证码识别 | `robot_verify` | 识别记录 | 记录编号、目标站点、验证码类型 |
| 采集限速 | `crawl_limit` | 限速策略 | 策略编号、目标站点、请求间隔 |
| 模板库 | `template_lib` | 采集模板 | 模板编号、站点类型、模板名称 |
| 调度编排 | `scheduler` | 调度计划 | 计划编号、关联作业、Cron表达式 |
| 采集计费 | `crawl_cost` | 用量记录 | 记录编号、采集站点、计费周期 |
| 数据归档 | `data_archive` | 归档策略 | 策略编号、数据范围、归档周期 |
| 差异检测 | `diff_check` | 差异记录 | 记录编号、目标站点、检测页面 |
| 导出任务 | `export_task` | 导出记录 | 记录编号、数据来源、导出格式 |
| 操作审计 | `audit_log` | 审计记录 | 记录编号、操作模块、操作人 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 采集数据投递链路

投递链路以“收讫台账”为准，批次版本在一个目标系统上只保留一份有效收讫凭证。

- `app/services/delivery_ledger.py`：投递资格、接收仓库幂等入仓、收执补发、
  待对账进度与抽样绑定的唯一判定处；业务键是
  `(批次编号, 批次版本, 目标系统)`。
- `app/integration.py`：`deliver_to_target` / `retry_delivery` /
  `recover_delivery` / `receipt_detail` / `sample_workbench` /
  `delivery_trail`。投递重试复用已有成功凭证；本地写回中断会留下“待对账”
  进度，恢复任务先向接收仓库核对再补全同一份凭证。
- `app/routers/delivery.py`：`/api/delivery/{批次}/{版本}/...` 提供回执详情、
  目标轨迹、抽样工作台和投递/重试/恢复/补发收执动作。
- 前端 `/delivery_receipt`：投递回执详情与抽样工作台，可核对每个目标的入仓
  次数、收讫凭证和样本数据版本。

演示数据 `B-A@v1` 已在校验通过状态，起服务后可直接投递到 `W1`、`W2`：

```bash
curl -X POST http://127.0.0.1:8000/api/delivery/B-A/v1/targets/W1/actions \
  -H 'Content-Type: application/json' -d '{"action":"投递"}'
curl http://127.0.0.1:8000/api/delivery/B-A/v1/targets/W1
curl http://127.0.0.1:8000/api/delivery/B-A/v1/trail
curl http://127.0.0.1:8000/api/delivery/B-A/v1/samples
```

模拟“回执后本地写回中断”：同一动作带上 `"interrupt_before": "receipt"`，
之后调用 `恢复投递`（或同时启动两个恢复执行）即可验证接收仓库只入仓一次、
本地只落一份凭证与一条投递任务。

领域：数据工程
子方向：数据采集、爬虫与RPA
