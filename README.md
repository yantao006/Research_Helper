# Research Helper

一个用于批量股票研究的 Python 小工具。

它会读取 `prompts.csv` 和 `tasks.csv`，按 `prompts.csv` 的顺序为每个 `analyzed=False` 的公司逐条调用大模型 API，并把结果写入 `output/{Ticker}_{YYYY-MM-DD}/` 目录。每家公司全部 prompt 成功后，才会把 `tasks.csv` 中对应行标记为已分析。

## 1. 准备文件

在当前目录放好两个 CSV 文件。

`prompts.csv`

```csv
id,question,prompt
1,公司简介,请概述 {company}（股票代码 {ticker}）的主营业务、主要市场、竞争位置，并基于 {date} 能查到的公开信息总结。
2,近期动态,请结合联网搜索，总结 {company}（{ticker}）在 {date} 前后值得关注的最新动态、财报、指引、产品或监管事件。
```

`tasks.csv`

```csv
company,Ticker,analyzed,analyzed_date
Apple,AAPL,False,
Microsoft,MSFT,False,
```

说明：

- `prompts.csv` 必须包含列：`id,question,prompt`
- `tasks.csv` 必须包含列：`company,Ticker,analyzed,analyzed_date`
- prompt 里可以使用占位符：`{company}`、`{ticker}`、`{date}`

## 2. 配置 Python

推荐 Python 3.10 或更高版本。

确认版本：

```bash
python3 --version
```

默认运行（`--repo-backend auto`）只依赖 Python 标准库。  
如果启用 Postgres 持久化（`--repo-backend postgres`），需要额外安装：

```bash
pip install "psycopg[binary]"
```

## 3. 选择模型提供方

脚本支持这些 provider：

- `openai`
- `siliconflow`
- `modelscope`
- `qwen`
- `doubao`
- `zhipu`

当前脚本的接口策略：

- `openai`：使用 Responses API，支持内置 `web_search`，并可提取结构化来源链接
- `doubao`：使用 Responses 风格接口，支持内置 `web_search`，但当前不提取 `include` 来源字段
- `siliconflow` / `modelscope` / `qwen` / `zhipu`：使用 OpenAI 兼容的 `chat/completions`

说明：

- 并不是所有 provider 都支持脚本里原来的“联网搜索”能力
- 对不支持 `web_search` 的 provider，脚本会自动继续执行，但不会附带内置联网工具
- 你仍然可以通过 prompt 要求模型基于已有知识做分析

## 4. 配置 API Key

复制示例环境文件：

```bash
cp .env.example .env
```

然后编辑 `.env`。默认 provider 是 `doubao`，如果你想先用 OpenAI，可以显式写成：

```env
MODEL_PROVIDER=openai
OPENAI_API_KEY=你的_API_Key
OPENAI_MODEL=gpt-5.2
```

如果你想切到其他 provider，可以这样配：

`SiliconFlow`

```env
MODEL_PROVIDER=siliconflow
SILICONFLOW_API_KEY=你的_SiliconFlow_Key
SILICONFLOW_MODEL=Qwen/Qwen2.5-72B-Instruct
```

`ModelScope`

```env
MODEL_PROVIDER=modelscope
MODELSCOPE_API_KEY=你的_ModelScope_Token
MODELSCOPE_MODEL=Qwen/Qwen3-32B
```

`阿里云百炼 / Qwen`

```env
MODEL_PROVIDER=qwen
DASHSCOPE_API_KEY=你的_DashScope_Key
QWEN_MODEL=qwen-plus
```

`火山引擎 / 豆包`

```env
MODEL_PROVIDER=doubao
ARK_API_KEY=你的_ARK_Key
DOUBAO_MODEL=doubao-seed-1-6-250615
```

`智谱 / GLM`

```env
MODEL_PROVIDER=zhipu
ZAI_API_KEY=你的_Zhipu_Key
ZHIPU_MODEL=glm-5
```

可选覆盖项：

- `LLM_MODEL`：统一覆盖当前 provider 的模型名
- `LLM_BASE_URL`：统一覆盖当前 provider 的 base URL
- `XXX_MODEL`：覆盖某个 provider 的默认模型
- `XXX_BASE_URL`：覆盖某个 provider 的默认 base URL

## 5. 运行程序

最简单的运行方式：

```bash
python3 research_batch/main.py
```

常用参数：

```bash
python3 research_batch/main.py \
  --prompts prompts.csv \
  --tasks tasks.csv \
  --output-root output \
  --provider openai \
  --model gpt-5.2 \
  --report-date 2026-03-20 \
  --max-retries 3 \
  --retry-delay 5
```

参数说明：

- `--report-date`：用于填充 prompt 中的 `{date}`，同时用于输出目录名
- `--provider`：切换模型提供方
- `--max-retries`：单个 prompt 失败后的最大重试次数
- `--retry-delay`：两次重试之间等待秒数
- `--request-timeout`：单次 API 请求超时时间，默认 600 秒
- `--disable-web-search`：即使 provider 支持联网工具，也强制关闭
- `--provider-test`：只测试当前 provider 配置是否可用，不处理 `tasks.csv`
- `--force-rerun`：即使结果文件已存在，也强制重新生成并覆盖
- `--feishu-sync-test`：只测试飞书同步连通性与写入能力，不处理 `tasks.csv`
- `--feishu-sync-only`：只把本地已有 `output` 结果同步到飞书，不调用模型
- `--feishu-async-flush-timeout`：程序退出时等待飞书异步队列刷新的最长秒数（默认 20）
- `--feishu-sync-max-retries`：飞书异步任务单公司最大重试次数（默认 3）
- `--feishu-sync-retry-delay`：飞书异步任务重试间隔秒数（默认 2）
- `--feishu-dead-letter`：飞书异步失败任务的 JSONL 落盘路径（默认 `logs/feishu_sync_dead_letter.jsonl`）
- `--repo-backend`：仓储后端，支持 `auto`（默认）/ `local` / `postgres` / `dual`
- `--postgres-dsn`：Postgres 连接串；当 `--repo-backend=postgres|dual` 时必填（也可用 `POSTGRES_DSN` / `DATABASE_URL`）
- `--dual-write-strict`：仅在 `dual` 模式下生效；开启后，Postgres 副写失败会直接中断任务
- `--router-config`：启用行业路由配置（如 `prompt_router.yaml`）
- `--industry-prompts`：指定行业专用 prompt CSV（默认读路由文件里的路径）
- `--profile`：路由执行档位，支持 `quick/standard/full/monitoring`
- `--industry-override`：强制行业分类（仅路由模式）
- `--dry-run-plan`：仅输出每家公司最终要执行的 prompt 计划，不落盘
- `--disable-seo-keyword-links`：关闭“二次提取关键词并追加站内软链接”的后处理
- `--seo-keyword-limit`：每篇文档最多提取多少个 SEO 关键词（默认 8）

`repo-backend=auto` 的行为（Phase 4）：

- 本地环境默认走 `local`
- 生产环境（`RESEARCH_ENV/APP_ENV/ENV/NODE_ENV=production`）默认强制走 `postgres`
- 如果生产环境要临时放行非 postgres，用 `ALLOW_NON_POSTGRES_IN_PRODUCTION=true`（仅建议应急）

飞书同步（Phase 5）：

- 常规批处理主流程中，飞书同步改为异步后台执行，不阻塞公司调研主流程
- 同步失败不会影响 `tasks.csv` 标记成功与否
- 多次重试后仍失败会写入 dead-letter 文件，便于后续补偿重放
- `--feishu-sync-only` 仍保留为同步直连模式，用于手动补偿

关键词检索（Phase 6）：

- 网站 `GET /api/keyword-search` 优先查询 Postgres（`rb_docs.answer_markdown + rb_seo_keywords`）
- 若数据库未配置或查询异常，会自动回退到本地 `output` 文件扫描
- `rb_seo_keywords` 在 Postgres schema 初始化时自动创建
- 每次文档落库（`save_research_doc`）会自动从 Markdown 的 `## SEO 关键词` 段提取并更新关键词表

示例：

```bash
python3 research_batch/main.py --provider siliconflow
python3 research_batch/main.py --provider qwen --model qwen-max
python3 research_batch/main.py --provider doubao
python3 research_batch/main.py --provider openai --disable-web-search
python3 research_batch/main.py --provider zhipu --provider-test
python3 research_batch/main.py --provider doubao --force-rerun
python3 research_batch/main.py --feishu-sync-test
python3 research_batch/main.py --feishu-sync-only --report-date 2026-03-21
python3 research_batch/main.py --repo-backend auto --postgres-dsn "postgresql://user:pass@host:5432/db"
python3 research_batch/main.py --repo-backend postgres --postgres-dsn "postgresql://user:pass@host:5432/db"
python3 research_batch/main.py --repo-backend dual --postgres-dsn "postgresql://user:pass@host:5432/db"
python3 research_batch/main.py --router-config prompt_router.yaml --profile standard --dry-run-plan
python3 research_batch/main.py --router-config prompt_router.yaml --profile full --provider doubao
python3 research_batch/main.py --provider doubao --force-rerun --seo-keyword-limit 10
```

路由行为（Phase 2）：

- `single_industry_company`：执行 profile 的通用 prompts + 行业 overlay
- `multi_segment_company`：执行核心通用 prompts `1,2,5,10,12` + 主行业 overlay
- `conglomerate_or_holding`：仅执行通用 prompts（不叠加行业）
- `low_confidence_classification`：仅执行通用 prompts，且 `manual_review=true`

路由行为（Phase 3 增强）：

- 分类器支持输出 `primary + secondary + industry_weights`
- `multi_segment_company` 会按权重选主行业，并混入次行业的一个关键 overlay prompt
- `--dry-run-plan` 会额外打印 `industry_mix` 与 `secondary`，便于人工验路由

建议先跑一次自检，再正式跑批量任务：

```bash
python3 research_batch/main.py --provider openai --provider-test
python3 research_batch/main.py --provider doubao --provider-test
```

## 6. 输出结果

输出目录结构示例：

```text
output/
  AAPL_2026-03-20/
    1_公司简介.md
    2_近期动态.md
```

每个 Markdown 文件包含：

- 问题标题
- 公司、Ticker、日期、模型
- 行业（在路由模式下自动写入）
- 模型回答
- 联网搜索返回的来源链接（如果有）

## 7. 回写规则

程序只处理 `analyzed=False` 的公司。

如果某家公司：

- 所有 prompt 都成功生成，程序会立刻把 `tasks.csv` 中该行更新为：
  - `analyzed=True`
  - `analyzed_date=当前时间`
- 中途有任意一个 prompt 失败，程序不会把这家公司标记为成功

这样你可以重复运行脚本，它会继续处理未完成项。

## 8. 断点续跑与不覆盖

为了便于追踪和重试，程序遵循下面这些规则：

- 已存在的输出文件不会被覆盖
- 如果某个 prompt 的结果文件已经存在，下一次运行会自动跳过
- `tasks.csv` 使用原子方式回写，降低中途写坏文件的风险
- 运行日志写入 `logs/research_helper.log`
- 若需要覆盖重跑，可使用 `--force-rerun`

## 9. 常见问题

`1. 运行时报 Missing required environment variable`

说明 `.env` 没配好，或者当前 shell 没读到环境变量。检查 `.env` 是否放在当前目录，并确认当前 provider 对应的 key 变量已经填写，例如 `OPENAI_API_KEY`、`SILICONFLOW_API_KEY`、`DASHSCOPE_API_KEY`、`ARK_API_KEY`、`ZAI_API_KEY`。

`2. 输出目录里已经有部分 md 文件，但 tasks.csv 还是 False`

这是正常的，说明之前有部分 prompt 成功、部分失败。重新运行即可，脚本会跳过已有结果，继续补剩余项。

`3. 想换 provider 或模型怎么办`

常见方式：

- 改 `.env` 里的 `MODEL_PROVIDER`
- 改 `.env` 里的 provider 对应模型变量
- 或运行时传 `--provider` 和 `--model`

例如：

```bash
python3 research_batch/main.py --provider zhipu --model glm-5
```

`4. 使用国内 provider 时为什么没有 Sources`

因为当前脚本里只有 `openai` 会提取结构化来源链接。`doubao` 虽然支持 Responses 风格接口和内置 `web_search`，但它当前不兼容脚本使用的 `include=web_search_call.action.sources` 参数，所以不会返回 `Sources` 列表。其余 provider 默认走 `chat/completions`，通常也不会返回结构化来源链接。

`5. 如何快速确认当前 provider 配置没问题`

先运行：

```bash
python3 research_batch/main.py --provider 你的provider --provider-test
```

它会发送一个极轻量的测试请求，并在日志里打印成功或失败原因。这样可以先确认 key、base URL、模型名和网络连通性，再开始批量研究。

`6. 豆包报 ToolNotOpen 怎么办`

如果账号未开通 web_search，脚本会自动降级为“无 web_search”继续执行。你也可以显式加上 `--disable-web-search`，避免触发工具权限报错。

## 10. 官方接口说明

本项目里 provider 预设基于各家官方文档的常见入口形式整理，建议你在正式使用前再核对一次账户可用模型与区域限制：

- [Responses API](https://platform.openai.com/docs/api-reference/responses)
- [OpenAI tools / web search](https://platform.openai.com/docs/guides/tools)
- [SiliconFlow API Docs](https://docs.siliconflow.com/)
- [ModelScope API Inference](https://www.modelscope.cn/docs/model-service/API-Inference/intro)
- [阿里云百炼 OpenAI 兼容模式](https://help.aliyun.com/zh/model-studio/compatibility-of-openai-with-dashscope)
- [火山引擎方舟 / Doubao](https://www.volcengine.com/docs/82379/1338552)
- [智谱 GLM OpenAI 兼容接口](https://docs.bigmodel.cn/cn/guide/develop/openai/introduction)

## 11. 可视化网站（Next.js）

项目已内置一个 Next.js 网站，用于展示 `output` 目录中的研究结果。

### 启动方式

```bash
npm install
npm run dev
```

默认访问地址：

`http://localhost:3000`

发布前可先跑一键体检：

```bash
npm run release:check
```

### 页面说明

- 首页：按运行目录展示公司卡片（公司、Ticker、日期、Provider）
- 详情页：展示该公司各研究问题的回答内容和来源链接（若有）

### 强制重跑后查看

如果你用了 `--force-rerun` 覆盖结果，刷新网页即可看到最新内容。网站会直接读取当前 `output` 目录。

### 生产发布安全开关（新增）

- `ENABLE_RESEARCH_JOBS=false`（推荐生产默认）
  - 关闭在线“发起调研”接口与轮询接口
  - 首页自动切换为只读展示模式，避免公网触发模型调用和本地写盘
- `QUOTES_REVALIDATE_SECONDS=45`（可选）
  - 控制行情接口缓存刷新间隔（秒）

## 12. 飞书同步（MVP）

脚本支持在公司研究完成后，把结果同步到飞书多维表格（按问题一行）。

### 配置项

在 `.env` 中增加：

```env
FEISHU_ENABLE_SYNC=true
FEISHU_APP_ID=你的飞书应用AppID
FEISHU_APP_SECRET=你的飞书应用AppSecret
FEISHU_APP_TOKEN=你的多维表格AppToken
FEISHU_TABLE_ID=你的数据表TableID
```

先做飞书自检（推荐）：

```bash
python3 research_batch/main.py --feishu-sync-test
```

该自检会验证：

- 飞书鉴权是否可用
- 表读取是否可用
- 记录创建与更新是否可用
- 最后尝试清理测试记录

只执行飞书同步（不跑模型，不改 `tasks.csv`）：

```bash
python3 research_batch/main.py --feishu-sync-only --report-date 2026-03-21
```

该模式会扫描 `tasks.csv` 中公司对应目录下的本地 markdown，按幂等规则同步到飞书。

### 同步时机

- 每家公司全部问题成功后，触发一次飞书同步
- 同步失败会重试 3 次，并写日志
- 同步失败不会影响本地 `output` 文件和 `tasks.csv` 回写

### 幂等规则

按 `sync_key=ticker|report_date|prompt_id` 做幂等：

- 已存在：更新记录
- 不存在：创建记录

### 飞书表字段建议

请在飞书多维表格中创建以下字段（名称需一致）：

- `sync_key`
- `company`
- `ticker`
- `industry`
- `report_date`
- `prompt_id`
- `question`
- `answer_markdown`
- `sources`
- `provider`
- `model`
- `output_path`
- `synced_at`

## 13. 对外发布（Next.js 平台）

### 推荐发布模式：只读展示

适合直接部署到 Vercel/Next.js 平台。

1. 在本地先完成调研并生成 `output/*` markdown  
2. 将 `output` 目录一并提交（或接入你自己的持久化存储层）  
3. 部署时设置：

```env
ENABLE_RESEARCH_JOBS=false
QUOTES_REVALIDATE_SECONDS=45
```

4. 不要在公开展示站点注入模型密钥（如 `OPENAI_API_KEY` / `ARK_API_KEY`）

### 为什么默认不开放在线调研

当前在线调研流程依赖：

- 写入 `tasks.csv`
- 写入 `output/*`
- 子进程执行 `python3 research_batch/main.py`

这类长任务 + 本地文件写入模式，不适合直接放在无状态 Serverless 环境中公网开放。  
如需公网在线调研，建议拆分为独立后端任务服务（队列 + 持久化存储 + 鉴权限流）。
