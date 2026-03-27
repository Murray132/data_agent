# DATA AGENT - 数据治理智能体平台 技术规格说明书

## 一、项目背景

### 1.1 项目概述

DATA AGENT 是一个基于大语言模型（LLM）的数据治理智能体平台，旨在通过AI技术自动化解决企业数据资产管理中的核心痛点。平台基于 **AgentScope** 智能体框架开发，采用 **FastAPI** 构建后端服务，提供Web界面供用户交互。

### 1.2 业务痛点

在企业数据管理实践中，普遍存在以下问题：

1. **元数据缺失**：数据库表和字段缺乏描述，新人难以理解业务含义
2. **SQL编写困难**：非技术人员难以编写复杂查询，效率低下
3. **数据资产混乱**：缺乏统一的标签体系，数据发现困难
4. **SQL质量问题**：语法错误、性能隐患难以及时发现

### 1.3 解决方案

DATA AGENT 通过4个核心AI智能体解决上述问题：

| 智能体 | 功能 | 解决的问题 |
|--------|------|-----------|
| **元数据补全Agent** | 自动生成表和字段的描述信息 | 元数据缺失 |
| **SQL生成Agent** | 自然语言转SQL查询 | SQL编写困难 |
| **数据资产打标Agent** | 自动为表和字段打标签 | 数据资产混乱 |
| **SQL纠错校验Agent** | 检查SQL语法和性能问题 | SQL质量问题 |

### 1.4 目标用户

- 数据分析师：快速理解数据、生成查询
- 数据工程师：自动化元数据管理
- 业务人员：自助式数据探索
- 数据治理团队：统一数据资产管理

### 1.5 技术选型

- **智能体框架**：AgentScope（支持ReAct模式）
- **大语言模型**：阿里云百炼 qwen3-max（通义千问）
- **后端框架**：FastAPI + Uvicorn
- **前端技术**：原生JavaScript + 现代化CSS
- **数据库**：SQLite（可扩展至MySQL/PostgreSQL）
- **部署方式**：本地启动 / Docker容器化

---

## 二、系统架构

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                        前端层 (Frontend)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  index.html │  │  style.css  │  │      app.js         │  │
│  │  (主页面)   │  │  (样式表)   │  │  (交互逻辑)         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP/REST API
┌──────────────────────────▼──────────────────────────────────┐
│                        接口层 (API Layer)                    │
│                    FastAPI + Pydantic                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  表管理API  │  │  元数据API  │  │   Agent调用API      │  │
│  │  /api/tables│  │/api/metadata│  │   /api/agent/*      │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      智能体层 (Agent Layer)                  │
│                   AgentScope ReActAgent                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ 元数据补全   │ │ SQL生成      │ │ 数据资产打标 │        │
│  │   Agent      │ │   Agent      │ │    Agent     │        │
│  └──────────────┘ └──────────────┘ └──────────────┘        │
│  ┌──────────────┐                                           │
│  │ SQL纠错校验  │                                           │
│  │    Agent     │                                           │
│  └──────────────┘                                           │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      服务层 (Service Layer)                  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              DatabaseService (数据库服务)              │  │
│  │  - 表结构查询  - 数据CRUD  - 元数据管理  - 标签管理    │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Skill Registry (技能注册中心)             │  │
│  │  - Database Schema Analysis Skill                     │  │
│  └───────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      数据层 (Data Layer)                     │
│  ┌──────────────────┐      ┌──────────────────────────────┐ │
│  │   SQLite数据库   │      │   外部模型API               │ │
│  │  data/finance.db │      │   - 阿里云百炼 DashScope    │ │
│  │                  │      │   - OpenAI API (备用)       │ │
│  └──────────────────┘      └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 模块职责

#### 2.2.1 前端层 (frontend/)

| 文件/目录 | 职责 |
|-----------|------|
| `templates/index.html` | 单页应用主页面，包含所有功能模块的UI结构 |
| `static/css/style.css` | 全局样式，包含现代化渐变、动画、响应式布局 |
| `static/js/app.js` | 前端交互逻辑，API调用，状态管理 |

**前端特性**：
- 单页应用（SPA）架构，标签页切换无刷新
- 流式数据展示（Server-Sent Events）
- 响应式布局，支持桌面和移动端
- 现代化UI设计，科技感渐变配色

#### 2.2.2 接口层 (backend/api/)

| API分类 | 端点 | 功能 |
|---------|------|------|
| **表管理** | `GET /api/tables` | 获取所有表列表 |
| | `GET /api/tables/{name}` | 获取表结构详情 |
| | `GET /api/tables/{name}/data` | 分页获取表数据 |
| **元数据** | `GET /api/metadata/{table}` | 获取表元数据 |
| | `POST /api/metadata/{table}` | 更新表/字段描述 |
| **Agent** | `POST /api/agent/metadata/generate` | 流式生成元数据 |
| | `POST /api/agent/metadata/apply` | 应用生成的元数据 |
| | `POST /api/agent/sql/generate` | 流式生成SQL |
| | `POST /api/agent/tagging/generate` | 流式生成标签 |
| | `POST /api/agent/tagging/apply` | 应用生成的标签 |
| | `POST /api/agent/validation/validate` | 流式校验SQL |

#### 2.2.3 智能体层 (backend/agents/)

所有Agent基于 **AgentScope.ReActAgent** 实现，采用统一的架构模式：

```python
class XXXAgent:
    - __init__: 初始化LLM模型、Memory、Tools
    - create_xxx_agent: 工厂方法创建Agent实例
    - generate_xxx: 主入口方法，协调Agent执行
    - 工具函数: 供Agent调用的数据库操作工具
```

**Agent通用配置**：
- 模型：qwen3-max（DashScope）
- 记忆：InMemoryMemory（会话级）
- 模式：ReAct（推理+行动）
- 工具注册：Toolkit + register_tool_function

#### 2.2.4 服务层 (backend/database/)

**DatabaseService** 提供统一的数据库操作接口：

| 功能类别 | 方法 |
|----------|------|
| 表结构 | `get_tables()`, `get_table_schema()`, `get_table_relationships()` |
| 数据操作 | `get_table_data()`, `execute_sql()` |
| 元数据 | `update_table_description()`, `update_column_description()` |
| 标签管理 | `add_table_tags()`, `add_column_tags()`, `get_table_tags()` |

**数据库表结构**：
- 业务表：由用户提供（如 finance.db 中的业务数据表）
- 元数据表：`_table_metadata`（表描述）、`_column_metadata`（字段描述）
- 标签表：`_table_tags`（表标签）、`_column_tags`（字段标签）

#### 2.2.5 技能层 (skills/)

**Database Schema Analysis Skill**：
- 独立目录：`skills/database-schema-analysis/`
- 包含：SKILL.md（使用说明）、analyze_schema.py（分析脚本）
- 被SQL生成Agent通过 `register_agent_skill` 方式调用

### 2.3 数据流

#### 2.3.1 元数据补全流程

```
用户选择表 → 点击"生成描述" → API流式调用Agent
    ↓
Agent分析：表结构 + 样本数据 + 关联关系
    ↓
流式返回：思考过程 + 生成结果
    ↓
前端展示 → 用户编辑 → 点击"应用" → 保存到数据库
```

#### 2.3.2 SQL生成流程

```
用户输入自然语言需求 → 点击"生成SQL"
    ↓
Agent执行：
  1. 搜索相关表（使用Database Schema Analysis Skill）
  2. 分析表结构和关联
  3. 生成SQL语句
  4. 验证语法正确性
    ↓
流式返回：分析过程 + 最终SQL + 解释说明
    ↓
用户可执行SQL查看结果
```

#### 2.3.3 数据资产打标流程

```
用户选择表 → 点击"生成标签"
    ↓
Agent分析表和字段的业务含义
    ↓
生成：表标签（3-5个）+ 每个字段的标签（各3个）
    ↓
流式展示生成过程
    ↓
用户确认 → 应用标签 → 保存到标签表
```

#### 2.3.4 SQL纠错校验流程

```
用户输入SQL → 点击"校验"
    ↓
Agent检查：
  - 语法正确性
  - 表和字段存在性
  - 性能隐患（如全表扫描）
  - 最佳实践（如索引建议）
    ↓
返回：校验结果（通过/问题）+ 修改建议 + 修正后SQL
```

### 2.4 技术特点

1. **流式响应**：所有Agent API采用SSE（Server-Sent Events）流式返回，实时展示AI思考过程
2. **工具扩展**：Agent通过Tool机制调用数据库操作，易于扩展新功能
3. **技能复用**：SQL生成Agent通过Skill方式复用数据库分析能力
4. **状态隔离**：每个Agent实例独立，会话级Memory管理
5. **模型可切换**：支持DashScope（通义千问）和OpenAI（GPT）两种模型后端

### 2.5 部署架构

```
开发环境：
  python run.py --port 8000

生产环境（可选）：
  Docker容器化部署
  Nginx反向代理
  SQLite → MySQL/PostgreSQL迁移
```

### 2.6 目录结构

```
data_agent/
├── config.py              # 全局配置（API密钥、模型配置）
├── run.py                 # 启动脚本
├── requirements.txt       # Python依赖
├── data/                  # 数据库文件
│   ├── finance.db        # 业务数据库
│   └── init_database.py  # 数据库初始化
├── backend/              # 后端代码
│   ├── api/
│   │   └── server.py     # FastAPI主服务
│   ├── agents/           # 智能体实现
│   │   ├── metadata_agent.py
│   │   ├── sql_agent.py
│   │   ├── tagging_agent.py
│   │   └── sql_validation_agent.py
│   └── database/
│       └── db_service.py # 数据库服务层
├── frontend/             # 前端代码
│   ├── templates/
│   │   └── index.html    # 主页面
│   └── static/
│       ├── css/style.css
│       └── js/app.js
└── skills/               # Agent技能
    └── database-schema-analysis/
        ├── SKILL.md
        └── analyze_schema.py
```

---

## 三、待补充章节（后续完善）

- [ ] 三、功能规格
- [ ] 四、API详细定义
- [ ] 五、数据库设计
- [ ] 六、前端交互设计
- [ ] 七、部署与运维
- [ ] 八、开发规范

---

**文档版本**：v1.0  
**编写日期**：2026-01-28  
**编写人**：AI Assistant（基于现有代码反向生成）
