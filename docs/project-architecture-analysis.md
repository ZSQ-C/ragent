# Ragent AI 后端架构与框架接口/注解详解

> 本文基于对项目源码的全量扫描（不含 `frontend/` 前端代码），分步骤讲解项目的整体架构、各模块职责、核心业务链路，以及所有被引用的**框架接口**与**注解**——每一条都标注了具体出处（类 + 文件路径），便于对照源码阅读。

---

## 目录

1. [项目概览与技术栈](#1-项目概览与技术栈)
2. [整体架构分层](#2-整体架构分层)
3. [bootstrap 模块详解（主应用）](#3-bootstrap-模块详解主应用)
4. [framework 模块详解（脚手架/横切能力）](#4-framework-模块详解脚手架横切能力)
5. [infra-ai 模块详解（AI 模型引擎）](#5-infra-ai-模块详解ai-模型引擎)
6. [mcp-server 模块详解（MCP 工具服务）](#6-mcp-server-模块详解mcp-工具服务)
7. [框架接口引用详解（重点）](#7-框架接口引用详解重点)
8. [注解引用详解（重点）](#8-注解引用详解重点)
9. [配置体系与 SPI 扩展点](#9-配置体系与-spi-扩展点)
10. [设计模式与工程亮点总结](#10-设计模式与工程亮点总结)

---

## 1. 项目概览与技术栈

Ragent（`com.nageoffer.ai:ragent:0.0.1-SNAPSHOT`）是一个面向 **Agentic RAG 演进**的企业级 RAG 平台，覆盖"文档入库 → 解析 → 分块 → 向量化 → 多路检索 → 重排 → 流式生成"完整链路，并集成意图识别、会话记忆、MCP 工具调用、模型自动降级等能力。

### 1.1 基础信息

| 项 | 值 | 出处 |
|---|---|---|
| JDK | 17 | `pom.xml` `<java.version>` |
| Spring Boot | 3.5.7 | `pom.xml` `<spring-boot.version>` |
| 打包方式 | pom 聚合（4 个子模块） | `pom.xml` `<modules>` |
| 代码风格 | Spotless + Apache 2.0 License Header | `pom.xml` build 插件 |

### 1.2 Maven 模块划分

```
ragent (父 POM)
├── bootstrap    # 主应用：RAG 问答、知识库、会话、审计、定时任务（可执行 jar）
├── framework    # 脚手架层：统一响应/异常/幂等/上下文/分布式 ID/MQ 适配
├── infra-ai     # AI 基础设施：LLM/Embedding/Rerank/VLM 客户端 + 模型路由熔断
└── mcp-server   # 独立 MCP Server：暴露业务工具（天气/工单/销售/联网搜索）
```

依赖方向：`bootstrap → infra-ai → framework`，`mcp-server` 独立部署（端口 9099）。

### 1.3 核心第三方依赖（根 `pom.xml` `<dependencyManagement>`）

| 依赖 | 版本 | 用途 |
|---|---|---|
| `spring-boot-dependencies` | 3.5.7 | 全家桶 BOM |
| `milvus-sdk-java` | 2.6.6 | Milvus 向量数据库客户端 |
| `tika-bom` | 3.2.3 | 多格式文档解析（PDF/Office 等） |
| `mybatis-plus-spring-boot3-starter` | 3.5.14 | ORM 增强（PostgreSQL） |
| `software.amazon.awssdk:s3` | 2.40.2 | S3 协议对象存储 |
| `aliyun-sdk-oss` | 3.18.5 | 阿里云 OSS |
| `sa-token-spring-boot3-starter` + `sa-token-redis-template` | 1.43.0 | 登录鉴权（Redis 会话） |
| `redisson-spring-boot-starter` | 4.0.0 | 分布式锁/限流/队列 |
| `rocketmq-spring-boot-starter` | 2.3.5 | 消息队列（事务消息） |
| `transmittable-thread-local` | 2.14.5 | 跨线程池上下文传递 |
| `io.modelcontextprotocol.sdk:mcp` | 1.1.2 | MCP 协议 SDK |
| `bizlog-sdk` | 3.0.6 | 操作日志（`@LogRecord`） |
| `okhttp` | 4.12.0 | AI 模型 HTTP/SSE 调用 |
| `commonmark` (+gfm-tables) | 0.22.0 | Markdown 解析 |
| `batik-transcoder/codec` | 1.18 | SVG 处理 |
| Hutool | 5.8.37 | 工具库 |

bootstrap 自身还引入了 **Elasticsearch 客户端**（关键词检索）、**PostgreSQL JDBC + pgvector**（向量库备选）、**spring-boot-starter-web**（SSE 流式）等（见 `bootstrap/pom.xml`）。

---

## 2. 整体架构分层

```
┌─────────────────────────── 前端 (frontend/, 不在本文范围) ───────────────────────────┐
│                                                                                      │
│  ┌──────────────────────────── bootstrap (主应用 :端口见 application.yaml) ────────┐  │
│  │ Controller 层   RAGChatController / KnowledgeBaseController / UserController…  │  │
│  │       │ (SSE 流式)     │ (@IdempotentSubmit 幂等)                                │  │
│  │ Service 层      RAGChatServiceImpl → StreamChatPipeline（主编排流水线）          │  │
│  │       │ 记忆加载 → 查询改写 → 意图识别 → 歧义引导 → 多路检索 → 重排 → 流式生成      │  │
│  │ core 层        parser / chunk / retrieval / vector / keyword / graph /        │  │
│  │                memory / prompt / mcp / rewrite / source / storage              │  │
│  │ MQ 层          事务消息生产(RocketMQProducerAdapter) + 3 个消费者(幂等消费)       │  │
│  └──────┬──────────────┬──────────────┬──────────────┬──────────────┬────────────┘  │
│         │              │              │              │              │               │
│  ┌──────▼──────┐ ┌────▼─────┐ ┌──────▼──────┐ ┌─────▼─────┐ ┌─────▼─────────┐      │
│  │  infra-ai   │ │ Milvus/  │ │Elasticsearch│ │ S3/OSS    │ │ mcp-server    │      │
│  │ LLM/Embed/  │ │ pgvector │ │ BM25 关键词  │ │ 对象存储   │ │ :9099 /mcp    │      │
│  │ Rerank/VLM  │ │ 向量库    │ │ 检索        │ │           │ │ MCP 工具服务   │      │
│  │ 路由+熔断    │ └──────────┘ └─────────────┘ └───────────┘ └───────────────┘      │
│  └──────┬──────┘                                                                    │
│  ┌──────▼─────────────────── framework (脚手架) ─────────────────────────────────┐   │
│  │ 统一响应 Result/Results · 全局异常 GlobalExceptionHandler · 错误码体系          │   │
│  │ 幂等 @IdempotentSubmit/@IdempotentConsume(AOP) · 用户上下文 UserContext(TTL)   │   │
│  │ 分布式 ID(雪花) · MQ 事务消息适配 · Trace 注解 @RagTraceNode · Redis 序列化     │   │
│  └───────────────────────────────────────────────────────────────────────────────┘   │
│  横向设施：MySQL(PostgreSQL) · Redis(Redisson) · RocketMQ · Sa-Token 会话             │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

- **问答主链路**：`RAGChatController` → `RAGChatServiceImpl` → `StreamChatPipeline`（`rag/service/pipeline/StreamChatPipeline.java`）。
- **文档入库链路**：`KnowledgeDocumentController` → `KnowledgeDocumentServiceImpl.startChunk` → RocketMQ 事务消息 → `KnowledgeDocumentChunkConsumer` → 解析/分块/向量化/多存储同步。
- **模型调用链路**：业务 → `RoutingLLMService/Embedding/Rerank`（模型路由）→ `ModelSelector`（候选选择）→ `ModelHealthStore`（熔断）→ 各厂商 `ChatClient` 实现（OkHttp SSE）。
- **工具调用链路**：非知识类意图 → `McpToolRegistry` → `LLMMcpParameterExtractor`（LLM 提参）→ `McpClientToolExecutor`（MCP 客户端）→ 独立进程 `mcp-server`。

---

## 3. bootstrap 模块详解（主应用）

### 3.1 启动类

`bootstrap/src/main/java/com/nageoffer/ai/ragent/RagentApplication.java`

```java
@SpringBootApplication
@EnableScheduling
@EnableLogRecord(tenant = "ragent", proxyTargetClass = true)
@MapperScan(basePackages = {
        "com.nageoffer.ai.ragent.rag.dao.mapper",
        "com.nageoffer.ai.ragent.ingestion.dao.mapper",
        "com.nageoffer.ai.ragent.knowledge.dao.mapper",
        "com.nageoffer.ai.ragent.user.dao.mapper",
        "com.nageoffer.ai.ragent.audit.dao.mapper"
})
public class RagentApplication {
    public static void main(String[] args) {
        SpringApplication.run(RagentApplication.class, args);
    }
}
```

逐注解解读：

- `@SpringBootApplication`：组合注解（`@SpringBootConfiguration` + `@EnableAutoConfiguration` + `@ComponentScan`），从本包 `com.nageoffer.ai.ragent` 向下扫描所有业务 Bean。
- `@EnableScheduling`：开启 `@Scheduled` 定时任务（供 `KnowledgeDocumentScheduleJob` 使用）。
- `@EnableLogRecord(tenant = "ragent", proxyTargetClass = true)`：美团/社区 bizlog-sdk 操作日志启动注解；`tenant` 标识租户，`proxyTargetClass=true` 强制 CGLIB 代理以保证服务类方法上的 `@LogRecord` 切面生效。
- `@MapperScan`：批量把 5 个包下的 MyBatis-Plus `BaseMapper` 接口注册为 Mapper Bean，免去每个 Mapper 上的 `@Mapper`。

### 3.2 包结构总览（约 30 个业务包）

```
com.nageoffer.ai.ragent
├── RagentApplication                     # 启动类
├── rag/            # ★ RAG 核心：问答/检索/会话/Trace/评测
│   ├── controller/   # RAGChatController(SSE)、ConversationController、IntentTreeController、
│   │                 # GraphController、RagTraceController、RAGSettingsController、
│   │                 # MessageFeedbackController、QueryTermMappingController、SampleQuestionController…
│   ├── service/      # RAGChatService + impl；pipeline/StreamChatPipeline(编排)、
│   │                 # handler/StreamChatEventHandler、StreamTaskManager、ratelimit/
│   ├── core/         # 业务内核（纯接口+实现，无 Web 依赖）：
│   │   ├── retrieval/  # 多路检索引擎 + 4 通道(Vector/Keyword/Graph/Web) + 4 后处理器
│   │   ├── vector/     # 向量库抽象 + Milvus/pgvector 双实现 + 装饰器 + 并行检索策略
│   │   ├── keyword/    # ES BM25 关键词检索
│   │   ├── graph/      # LightRAG 图谱查询
│   │   ├── memory/     # 会话记忆（存储/摘要，JDBC 实现）
│   │   ├── prompt/     # Prompt 模板加载/组装（resources/prompt/*.st）
│   │   ├── intent/     # 意图树分类器/解析器/缓存
│   │   ├── guidance/   # 歧义引导（LLM 歧义检查）
│   │   ├── mcp/        # MCP 客户端自动装配 + 工具注册/提参/执行
│   │   ├── rewrite/    # 查询改写 + QueryTermMapping 词映射
│   │   ├── source/     # 引用来源组装
│   │   └── storage/    # S3/OSS 对象存储抽象
│   ├── mq/           # MessageFeedbackConsumer（点赞点踩）
│   ├── eval/         # 评测接口 EvalController/EvalProperties
│   ├── trace/        # RagStreamTraceSupportImpl（RAG 全链路追踪落库）
│   ├── config/       # WebConfig、MilvusConfig、EsClientConfig、HttpClientConfig、
│   │                 # StorageClientConfig、ThreadPoolExecutorConfig、 Utf8ResponseFilter、
│   │                 # DemoModeInterceptor、*PostProcessor、validation/(启动期校验)、
│   │                 # 各 *Properties 配置类
│   ├── dao/          # 11 张表的 Mapper/Entity（会话、消息、反馈、Trace、意图树…）
│   ├── dto/enums/    # SSEEventType、IntentKind/IntentLevel 等
│   └── util/
├── knowledge/      # ★ 知识库：文档 CRUD、分块管理、定时刷新
│   ├── service/impl/ # KnowledgeBaseServiceImpl、KnowledgeDocumentServiceImpl、
│   │                 # KnowledgeChunkServiceImpl、KnowledgeDocumentScheduleServiceImpl
│   ├── mq/           # 分块消费者/清理消费者 + 2 个事务消息回查 Checker
│   ├── schedule/     # KnowledgeDocumentScheduleJob(@Scheduled) + 分布式锁管理
│   ├── dao/handler/  # JsonbTypeHandler 等 4 个自定义 TypeHandler
│   └── config/ filter/ enums/
├── core/           # ★ 文档处理内核（与业务解耦）
│   ├── parser/       # DocumentParser 接口 + Tika/Markdown/CSV/Excel/Image/MinerU 6 实现
│   │                 # model/Block 体系（段落/标题/表格/图片/代码块）
│   └── chunk/        # ChunkingStrategy + 固定长度/结构感知 2 策略 + blockaware 分块器
├── ingestion/      # ★ 文档摄取流水线：node/ 7 个节点(Fetcher→Parser→Chunker→
│                   #   Enricher→Enhancer→Indexer) + task/pipeline 服务 + 飞书/URL 抓取器
├── user/           # 用户/认证：Sa-Token 配置、登录拦截器、UserController/AuthController
├── audit/          # 操作审计：bizlog 落库 + BizChangeLogController 查询
└── admin/          # 运营看板 DashboardController
```

### 3.3 问答主链路（逐步）

1. **入口**：`RAGChatController`（`rag/controller/RAGChatController.java`）暴露流式对话与停止任务接口，方法上标 `@IdempotentSubmit` 防重复提交，返回值用 `Results.success()` 统一包装。
2. **服务编排**：`RAGChatServiceImpl`（`rag/service/impl/`）——创建会话/任务 ID、构造 `StreamCallback`、经 `ChatQueueLimiter` + `FairDistributedRateLimiter`（Redisson + Lua `lua/queue_claim_atomic.lua`）公平排队限流，然后调 `StreamChatPipeline.execute(ctx)`。
3. **流水线**：`StreamChatPipeline` 依次执行
   `loadMemory`（会话记忆/摘要）→ `rewriteQuery`（多问题改写）→ `resolveIntents`（树形意图识别，`IntentClassifier`）→ `handleGuidance`（置信度不足时歧义引导 `IntentGuidanceService`）→ `retrieve`（`MultiChannelRetrievalEngine` 并行走向量/关键词/图谱/网页 4 通道，再经去重→RRF 融合→Rerank→元数据补全 4 个后处理器）→ `streamRagResponse / streamLLMResponse`（Prompt 组装 + 调 `LLMService` 流式生成）。
4. **输出**：`StreamChatEventHandler`（实现 infra-ai 的 `StreamCallback`）把 delta/reasoning/completed 翻译成 `SSEEventType` 事件，经 framework 的 `SseEmitterSender` 推给前端。
5. **追踪**：流水线方法标 `@RagTraceNode`，由 `RagStreamTraceSupportImpl` 采集节点耗时/输入输出，落库 `t_rag_trace_run / t_rag_trace_node`，供 `RagTraceController` 回放。

### 3.4 文档入库链路（逐步）

1. `KnowledgeDocumentController` 上传文档 → `KnowledgeDocumentServiceImpl`（`@LogRecord` 审计）把文件写入 `ObjectStorageClient`（S3 或 OSS），落库后 `startChunk` 通过 `RocketMQProducerAdapter` 发送**事务消息** `KnowledgeDocumentChunkEvent`。
2. `KnowledgeDocumentChunkConsumer`（`@RocketMQMessageListener` + `@IdempotentConsume`）消费，设置 `UserContext` 后执行 `executeChunk`。
3. `core/parser/DocumentParserSelector` 按文件类型路由到 6 种解析器之一（Tika/Markdown/CSV/Excel/图片 VLM/MinerU），产出 `ParsedDocument`（Block 结构保留表格/图片/代码块溯源 `Provenance`）。
4. `core/chunk/StructuredChunkingService` 按 `ChunkingStrategy`（固定长度 or 结构感知）分块，`blockaware` 包按 Block 类型分派专用分块器（表格/代码/图片…）。
5. `ChunkEmbeddingService` 调 `RoutingEmbeddingService.embedBatch` 向量化 → 写入 `VectorStoreService`（Milvus 或 pgvector）。
6. 装饰器同步：`KeywordSyncingVectorStoreService`（同步 ES 索引）、`GraphSyncingVectorStoreService`（同步 LightRAG 图谱）——由 `KeywordSyncVectorStorePostProcessor / GraphSyncVectorStorePostProcessor`（`BeanPostProcessor`）在容器启动时自动包装。
7. 知识库删除走 `KnowledgeBaseCleanupConsumer` 异步清理向量空间、对象存储、ES 索引、图谱数据，失败抛异常触发 MQ 重试。

---

## 4. framework 模块详解（脚手架/横切能力）

单模块（`framework/pom.xml`），依赖 Spring Boot Web、Redis、Redisson、MyBatis-Plus、Sa-Token、RocketMQ、Hutool。包结构与职责：

| 包 | 核心类 | 职责 |
|---|---|---|
| `convention` | `Result<T>`（implements Serializable）、`Results`、`ChatRequest/ChatMessage`、`RetrievedChunk/GroundingChunk/SourceRef` | 统一返回结构 `code/message/data/requestId` + 全项目共享的 RAG 领域值对象 |
| `web` | `GlobalExceptionHandler`、`SseEmitterSender` | `@RestControllerAdvice` 全局异常兜底；SSE 发送封装 |
| `errorcode` | `IErrorCode`（接口）、`BaseErrorCode`（枚举实现） | 错误码体系：客户端/系统/第三方三类 |
| `exception` | `AbstractException` + `ClientException/ServiceException/RemoteException` | 三级业务异常体系，供全局处理器按类分流 |
| `idempotent` | `@IdempotentSubmit` + `IdempotentSubmitAspect`；`@IdempotentConsume` + `IdempotentConsumeAspect`；`SpELUtil` | 接口防重复提交（Redisson 锁）+ MQ 消费幂等（Redis Lua） |
| `trace` | `@RagTraceNode`、`RagTraceContext`、`RagStreamTraceSupport` | RAG 链路追踪的注解与上下文 SPI |
| `mq` | `MessageQueueProducer`（接口）、`RocketMQProducerAdapter`（实现）、`DelegatingTransactionListener`（`@RocketMQTransactionListener`）、`TransactionChecker`、`MessageWrapper<T>` | 屏蔽 RocketMQ API 的统一生产者 + 本地事务消息回查委托 |
| `config` | `WebAutoConfiguration`、`DataBaseConfiguration`、`RocketMQAutoConfiguration` | 自动装配：全局异常 Bean、MyBatis-Plus 分页插件 + 字段填充器、MQ 生产者 |
| `database` | `MyMetaObjectHandler`（implements `MetaObjectHandler`） | insert 自动填 `createTime`，update 填 `updateTime`，逻辑删除 `deleted` |
| `distributedid` | `CustomIdentifierGenerator`（implements MyBatis-Plus `IdentifierGenerator`）、`SnowflakeIdInitializer` | 雪花算法自定义主键生成（workerId 管理） |
| `cache` | `RedisKeySerializer`（implements `RedisSerializer<String>`） | Redis key 加统一前缀 |
| `context` | `ApplicationContextHolder`（implements `ApplicationContextAware`）、`UserContext`、`LoginUser` | 静态获取容器；基于 TTL（TransmittableThreadLocal）的用户上下文，可跨线程池传递 |
| `web/GlobalExceptionHandler` 内部 | `@Value` 读取 multipart 上传大小 | 异常文案提示精确到配置的限制值 |

---

## 5. infra-ai 模块详解（AI 模型引擎）

### 5.1 四类能力的"接口 → 抽象 → 多厂商实现 → 路由"结构

| 能力 | 客户端接口 | 抽象基类 | 厂商实现 | 路由服务（@Primary） |
|---|---|---|---|---|
| Chat | `chat/ChatClient.java` | `AbstractOpenAIStyleChatClient` | `BaiLianChatClient`、`SiliconFlowChatClient`、`AIHubMixChatClient`、`OllamaChatClient` | `RoutingLLMService implements LLMService` |
| Embedding | `embedding/EmbeddingClient.java` | `AbstractOpenAIStyleEmbeddingClient` | `SiliconFlowEmbeddingClient`、`OllamaEmbeddingClient`、`AIHubMixEmbeddingClient` | `RoutingEmbeddingService implements EmbeddingService` |
| Rerank | `rerank/RerankClient.java` | —（BaiLian 直连） | `BaiLianRerankClient`、`NoopRerankClient`（空实现兜底） | `RoutingRerankService implements RerankService` |
| VLM | `vlm/VlmService.java` | — | — | `RoutingVlmService implements VlmService` |

### 5.2 模型调度三件套（生产级高可用设计）

- **`ModelSelector`**（`infra/model/ModelSelector.java`）：根据 `AIModelProperties` 为每次调用解析候选 `ModelTarget` 列表——chat 按档位（tier：快/强/深度思考）+ preferred 模型优先级排序；embedding/rerank/vlm 按 `defaultModel`、`priority` 排序；过滤未启用/不支持思考的模型，并把超时预算下沉到 `ModelTarget`。
- **`ModelHealthStore`**（`infra/model/ModelHealthStore.java`）：**断路器模式**，`CLOSED / OPEN / HALF_OPEN` 三态；成功调用重置失败计数，连续失败超过阈值（`ai.failure-threshold`）打开熔断，超过 `breaker-open-duration` 后进入半开试探。
- **`ModelRoutingExecutor`**（`infra/model/ModelRoutingExecutor.java`）：按候选列表依次"解析 client → 检查熔断状态 → 执行 `ModelCaller`"，失败自动 fallback 到下一个模型，全部失败抛 `RemoteException`。

流式场景额外有**首包探测**：`LlmFirstPacketProbe` + `ProbeStreamBridge`（implements `StreamCallback`）——模型迟迟不发首包/首包无内容视为失败，切换下一候选，保证"模型故障不影响服务"。

### 5.3 配置映射

`config/AIModelProperties.java`：`@ConfigurationProperties(prefix = "ai")`，含 provider（baseUrl/apiKey/endpoints）、chat/embedding/rerank/vlm 四组模型定义（candidates、tier、dimension、priority、enabled、reasoning 支持）、选择策略、流式响应参数、熔断阈值与打开时长。对应 `bootstrap` 的 `application.yaml` 中 `ai.*` 段。

---

## 6. mcp-server 模块详解（MCP 工具服务）

独立 Spring Boot 应用（`application.yml`：`server.port=9099`，应用名 `ragent-mcp-server`），基于官方 MCP Java SDK（`io.modelcontextprotocol.sdk:mcp:1.1.2`）实现 **Streamable HTTP** 传输的服务端。

### 6.1 服务装配：`mcp/config/McpServerConfig.java`

```java
@Bean
public HttpServletStreamableServerTransportProvider transportProvider() { … }

@Bean
public ServletRegistrationBean<HttpServletStreamableServerTransportProvider> mcpServlet(…) {
    return new ServletRegistrationBean<>(transportProvider, "/mcp");   // MCP 端点
}

@Bean
public McpSyncServer mcpServer(transportProvider, List<SyncToolSpecification> toolSpecs) {
    return McpServer.sync(transportProvider)
            .serverInfo("ragent-mcp-server", "0.0.1")
            .tools(toolSpecs)
            .build();
}
```

三个 Bean 分别是：传输层（Servlet 实现）、把传输层挂到 `/mcp` 路径、以同步模式创建 MCP Server 并注册所有工具。

### 6.2 工具实现：`executor/` 下 4 个执行器

`YouComSearchMcpExecutor`（联网搜索）、`WeatherMcpExecutor`（天气）、`TicketMcpExecutor`（工单）、`SalesMcpExecutor`（销售数据），模式统一：

- `@Bean youComSearchToolSpecification()` 返回 `McpServerFeatures.SyncToolSpecification`，把工具定义与 handler 绑定；
- 定义 `Tool`（名称 `youcom_search`、描述、`JsonSchema` 的 `inputSchema` 参数与必填项）；
- handler `handleCall(CallToolRequest)`：解析参数 → 校验 → OkHttp 调用真实 API → 包装 `CallToolResult` 返回。

### 6.3 主应用如何消费（MCP 客户端侧）

`bootstrap/rag/core/mcp/McpClientAutoConfiguration.java`：`@EnableConfigurationProperties(McpClientProperties.class)`，用 `HttpClientStreamableHttpTransport` + `McpClient.sync(...)` 连接 `http://…:9099/mcp`，`listTools()` 后把远程工具注册进 `DefaultMcpToolRegistry`；问答链路中由 `LLMMcpParameterExtractor`（LLM 按提示词 `prompt/mcp-parameter-extract.st` 提取参数）→ `McpClientToolExecutor` 执行调用，结果与知识库检索结果混合组装（Prompt：`answer-chat-mcp.st`、`answer-chat-mcp-kb-mixed.st`）。

---

## 7. 框架接口引用详解（重点）

> 分为两大部分：**A. 项目定义、供其他模块实现的接口**（业务抽象边界）；**B. 项目对第三方框架接口的 implements/extends**（扩展点接入）。每条均给出实现/使用位置。

### 7.1 项目内定义的业务接口（接口 → 实现类）

| 接口（定义路径） | 实现类（路径） | 作用 |
|---|---|---|
| `rag/service/RAGChatService` | `rag/service/impl/RAGChatServiceImpl` | 流式问答入口：`streamChat()` / `stopTask()` |
| `rag/service/FileStorageService` | `rag/service/impl/DefaultFileStorageService` | 文件上传存储门面 |
| `rag/service/ConversationService` 及 `ConversationMessageService`、`ConversationGroupService` | 同名 `impl` | 会话/消息/分组管理（记忆持久化的上层） |
| `rag/core/retrieval/RetrievalEngine` | `MultiChannelRetrievalEngine` | 多通道并行检索编排，输出统一 `RetrievedChunk` |
| `rag/core/retrieval/channel/SearchChannel` | `VectorSearchChannel` / `KeywordSearchChannel` / `GraphSearchChannel` / `WebSearchChannel` | 4 条检索通道的策略接口（策略模式） |
| `rag/core/retrieval/postprocessor/SearchResultPostProcessor` | `DeduplicationPostProcessor`、`FusionPostProcessor`（RRF 融合）、`RerankPostProcessor`、`MetadataEnrichmentPostProcessor` | 检索结果后处理责任链 |
| `rag/core/vector/VectorStoreService` / `VectorStoreAdmin` / `VectorRetrieverService` | `MilvusVectorStoreService/Admin/Retriever`、`PgVectorStoreService/Admin/Retriever`；装饰器 `KeywordSyncingVectorStoreService`、`GraphSyncingVectorStoreService` | 向量库三能力抽象，双数据库实现 + 装饰器增强（写向量同步写 ES/图谱） |
| `rag/core/vector/strategy/*` | `AbstractParallelRetriever` 抽象 + `IntentParallelRetriever` / `CollectionParallelRetriever` | 按意图/按集合并行召回的模板方法 |
| `rag/core/keyword/KeywordRetrieverService`、`KeywordIndexService` | `EsKeywordRetrieverService`、`EsKeywordIndexService` | ES BM25 关键词检索/索引 |
| `rag/core/memory/ConversationMemoryStore`、`ConversationMemoryService`、`ConversationMemorySummaryService` | `JdbcConversationMemoryStore`、`DefaultConversationMemoryService`、`JdbcConversationMemorySummaryService` | 会话记忆存取与摘要压缩 |
| `rag/core/intent/IntentClassifier`、`IntentNodeRegistry` | `DefaultIntentClassifier implements IntentClassifier, IntentNodeRegistry`（一个类实现两个接口） | 意图分类 + 意图树注册 |
| `rag/core/rewrite/QueryRewriteService` | `MultiQuestionRewriteService` | LLM 多问题改写 |
| `rag/core/prompt/ContextFormatter` | `DefaultContextFormatter` | 检索上下文 → Prompt 文本格式化 |
| `rag/core/storage/ObjectStorageClient` | `S3ObjectStorageClient`（AWS SDK）、`OssObjectStorageClient`（阿里云 SDK） | 对象存储抽象，双实现按配置切换 |
| `rag/core/mcp/McpToolRegistry` / `McpToolExecutor` / `McpParameterExtractor` | `DefaultMcpToolRegistry` / `McpClientToolExecutor` / `LLMMcpParameterExtractor` | MCP 工具注册表、执行器、LLM 参数抽取 |
| `core/parser/DocumentParser` | `TikaDocumentParser`、`MarkdownDocumentParser`、`CsvDocumentParser`、`ExcelDocumentParser`、`ImageDocumentParser`、`MinerUDocumentParser` | 6 种文档解析器（`DocumentParserSelector` 按类型路由） |
| `core/parser/model/Block` | `ParagraphBlock`、`HeadingBlock`、`TableBlock`、`ListBlock`、`ImageBlock`、`CodeBlock` | 解析产物的结构化块模型 |
| `core/chunk/ChunkingStrategy` + `ChunkingOptions` | `FixedSizeTextChunker` + `FixedSizeOptions`；`StructureAwareTextChunker` + `TextBoundaryOptions` | 两种分块策略（工厂：`ChunkingStrategyFactory`） |
| `core/chunk/blockaware/BlockChunker<T>` | `ParagraphChunker`、`TableChunker`、`CodeChunker`、`ListChunker`、`ImageChunker` | 按 Block 类型的专用分块器（泛型接口） |
| `ingestion/node/IngestionNode` | `FetcherNode`、`ParserNode`、`ChunkerNode`、`EnricherNode`、`EnhancerNode`、`IndexerNode` | 摄取流水线节点接口（pipeline 模式） |
| `ingestion/strategy/fetcher/DocumentFetcher` | `HttpUrlFetcher`、`FeishuFetcher` | 远程文档抓取（网页/飞书云文档） |
| `ingestion/service/IngestionTaskService` / `IngestionPipelineService` / `IntentTreeService` | 同名 `impl`（`IntentTreeServiceImpl` 同时 `extends ServiceImpl<IntentNodeMapper, IntentNodeDO>`） | 摄取任务/流水线/意图树管理 |
| `user/service/UserService`、`AuthService` | `UserServiceImpl`、`AuthServiceImpl` | 用户 CRUD 与登录认证 |
| `rag/service/ratelimit/*` | `FairDistributedRateLimiter`、`ChatQueueLimiter` | 分布式公平排队限流 |
| `framework/mq/producer/MessageQueueProducer` | `RocketMQProducerAdapter` | 屏蔽 MQ 细节的统一生产者接口 |
| `framework/errorcode/IErrorCode` | `BaseErrorCode`（枚举） | 错误码：`code()` + `message()` |
| `framework/trace/RagStreamTraceSupport` | `rag/trace/RagStreamTraceSupportImpl`（内部类 `StreamSpanImpl implements StreamSpan`） | Trace 采集 SPI，bootstrap 用它落库 |
| `infra/chat/ChatClient` / `LLMService`、`EmbeddingClient` / `EmbeddingService`、`RerankClient` / `RerankService`、`VlmService` | 见第 5 节表格 | AI 能力抽象（接口 → 路由实现 → 厂商客户端） |
| `infra/chat/StreamCallback` | `ProbeStreamBridge`、`ForwardingStreamCallback`（抽象）、bootstrap `StreamChatEventHandler` | 流式回调：onDelta/onReasoning/onCompleted/onError |
| `infra/chat/StreamCancellationHandle` | `StreamCancellationHandles.OkHttpCancellationHandle`（私有静态内部类） | 流式取消：底层 cancel OkHttp Call |
| `infra/model/ModelCaller` | lambda/方法引用传入 `ModelRoutingExecutor` | 函数式接口：一次模型调用动作 |
| `infra/token/TokenCounterService` | `HeuristicTokenCounterService` | 启发式 token 估算（Prompt 长度控制） |

### 7.2 对第三方框架接口的实现（扩展点接入）

| 第三方接口 | 实现类（路径） | 为什么实现它 |
|---|---|---|
| Spring `WebMvcConfigurer` | `user/config/SaTokenConfig`、`rag/config/WebConfig` | 注册拦截器（登录校验/体验模式/用户上下文/演示模式）与 CORS 等 MVC 定制 |
| Spring `HandlerInterceptor` | `user/config/UserContextInterceptor`、`rag/config/DemoModeInterceptor` | 前者 `preHandle` 从 Sa-Token 取登录用户写入 `UserContext`、`afterCompletion` 清理防线程复用污染；后者拦截演示模式的写操作 |
| Jakarta Servlet `Filter` | `rag/config/Utf8ResponseFilter`、`knowledge/filter/UploadRateLimitFilter` | 响应强制 UTF-8；上传接口限流 |
| Spring `BeanPostProcessor` | `rag/config/KeywordSyncVectorStorePostProcessor`、`GraphSyncVectorStorePostProcessor` | 容器启动时把向量库 Bean 动态包装成"写向量同步写 ES/图谱"的装饰器——不改业务代码即可增强 |
| Spring `InitializingBean` | `rag/config/SearchChannelProperties`、`infra/model/ChatTierConfigValidator` | 属性绑定完成后做启动期校验（通道配置合法性、模型档位配置合法性），错误 fail-fast |
| Spring `EnvironmentPostProcessor` + `Ordered` | `rag/config/validation/RetrievalConfigEnvironmentPostProcessor`（经 `META-INF/spring.factories` 注册） | 启动最前置阶段校验检索通道配置矛盾 |
| Spring Boot `FailureAnalyzer` | `rag/config/validation/RetrievalConfigFailureAnalyzer`（spring.factories 注册） | 把 `RetrievalConfigException` 渲染成"APPLICATION FAILED TO START"友好诊断框 |
| Spring `ApplicationContextAware` | `framework/context/ApplicationContextHolder` | 容器刷新时缓存 `ApplicationContext`，供非 Spring 管理对象静态取 Bean |
| MyBatis-Plus `MetaObjectHandler` | `framework/database/MyMetaObjectHandler` | insert/update 自动填充 `createTime/updateTime/deleted` |
| MyBatis-Plus `IdentifierGenerator` | `framework/distributedid/CustomIdentifierGenerator` | 自定义雪花 ID 生成器，配合 `@TableId(type = IdType.ASSIGN_ID)` |
| MyBatis-Plus `BaseMapper<T>` | 全部 20+ 个 Mapper（如 `KnowledgeBaseMapper`、`UserMapper`） | 免写基础 CRUD SQL |
| MyBatis-Plus `IService<T>` / `ServiceImpl<M, T>` | `ingestion/service/impl/IntentTreeServiceImpl` | 意图树批量操作复用 MP 通用服务层 |
| RocketMQ `RocketMQListener<T>` | `rag/mq/MessageFeedbackConsumer implements RocketMQListener<MessageWrapper<MessageFeedbackEvent>>`、`knowledge/mq/KnowledgeDocumentChunkConsumer`、`KnowledgeBaseCleanupConsumer` | 注解驱动消费：方法 `onMessage(T)` 直接收类型化消息 |
| RocketMQ `RocketMQLocalTransactionListener` | `framework/mq/producer/DelegatingTransactionListener`（`@RocketMQTransactionListener`） | 事务消息回查：委托给各业务 `TransactionChecker` 实现（`KnowledgeDocumentChunkTransactionChecker`、`KnowledgeBaseCleanupTransactionChecker`） |
| Sa-Token `StpInterface` | `user/config/SaTokenStpInterfaceImpl` | 提供用户权限/角色数据源，供鉴权 API 查询 |
| bizlog `IOperatorGetService` | `audit/service/impl/RagentOperatorGetService` | 告诉操作日志框架"当前操作人是谁"（从 `UserContext` 取） |
| bizlog `ILogRecordService` | `audit/service/impl/BizChangeLogRecordService` | `@LogRecord` 产生的日志落库（`t_biz_change_log`） |
| Jakarta Validation `ConstraintValidator<ValidMemoryConfig, MemoryProperties>` | `rag/config/validation/MemoryConfigValidator` | 自定义注解 `@ValidMemoryConfig` 的校验逻辑 |
| `Serializable` | `framework/convention/Result`、`framework/mq/MessageWrapper`、`rag/mq/event/MessageFeedbackEvent` 等 | MQ 消息体与返回体序列化 |
| `AutoCloseable` | `ingestion/util/HttpClientHelper`（内部资源类）、`knowledge/handler/RemoteFileFetcher.RemoteFetchResult` | 抓取资源 try-with-resources 释放 |
| MCP SDK `McpSyncServer`（由 `McpServer.sync()` 构建器返回） | `mcp-server/config/McpServerConfig` | 服务端会话与工具调度核心对象 |

---

## 8. 注解引用详解（重点）

### 8.1 项目自定义注解（4 个）

| 注解 | 定义路径 | 元注解/属性 | 配套机制 | 使用位置 |
|---|---|---|---|---|
| `@IdempotentSubmit` | `framework/src/main/java/com/nageoffer/ai/ragent/framework/idempotent/IdempotentSubmit.java` | `@Target(METHOD)` `@Retention(RUNTIME)`；`key`（SpEL，可选）、`message`（失败提示，默认"您操作太快，请稍后再试"） | `IdempotentSubmitAspect`（`@Aspect`）：Redisson 分布式锁，锁 key = 用户 + 请求路径 + 参数 MD5，SpEL key 优先 | `rag/controller/RAGChatController` 流式对话/停止任务接口 |
| `@IdempotentConsume` | 同包 `IdempotentConsume.java` | `@Target(METHOD)` `@Retention(RUNTIME)`；`keyPrefix`、`key`（SpEL，必填）、`keyTimeout`（秒，默认 3600） | `IdempotentConsumeAspect`：Redis Lua 脚本原子判断"未消费→置已消费"，防 MQ 重复消费 | `KnowledgeDocumentChunkConsumer`、`KnowledgeBaseCleanupConsumer`、`MessageFeedbackConsumer` |
| `@RagTraceNode` | `framework/src/main/java/com/nageoffer/ai/ragent/framework/trace/RagTraceNode.java` | `@Target(METHOD)` `@Retention(RUNTIME)`；`name`（节点展示名）、`type`（分组统计，默认 "METHOD"） | `RagStreamTraceSupportImpl` + `RagTraceContext` 采集节点执行轨迹，落库 Trace 表 | `rag/service/pipeline/StreamChatPipeline` 各流水线阶段方法 |
| `@ValidMemoryConfig` | `bootstrap/src/main/java/com/nageoffer/ai/ragent/rag/config/validation/ValidMemoryConfig.java` | `@Target(TYPE)` `@Retention(RUNTIME)` `@Constraint(validatedBy = MemoryConfigValidator.class)` `@Documented`；JSR-303 强制的 `message/groups/payload` | Jakarta Validation 启动校验 `MemoryProperties` 摘要配置合理性 | `rag/config/MemoryProperties` 类上 |

### 8.2 Spring / Spring Boot 注解

| 注解 | 在本项目中的使用位置与用途 |
|---|---|
| `@SpringBootApplication` | `RagentApplication`、`mcp/McpServerApplication` —— 自动装配 + 组件扫描入口 |
| `@EnableScheduling` | `RagentApplication` —— 启用 `@Scheduled` 定时任务 |
| `@Configuration` + `@Bean` | 全项目配置类，如 `rag/config/MilvusConfig`（Milvus 客户端）、`EsClientConfig`（ES 客户端）、`HttpClientConfig`（`@Primary OkHttpClient`）、`StorageClientConfig`（S3/OSS 客户端）、`ThreadPoolExecutorConfig`（流式线程池）、`ChatRateLimiterConfig`、`framework/config/WebAutoConfiguration`、`mcp/config/McpServerConfig` |
| `@EnableConfigurationProperties` | `rag/core/mcp/McpClientAutoConfiguration` 上启用 `McpClientProperties` |
| `@RestController` / `@GetMapping` / `@PostMapping` 等 | 15+ 个 Controller：`RAGChatController`、`KnowledgeBaseController`、`KnowledgeDocumentController`、`KnowledgeChunkController`、`ConversationController`、`IntentTreeController`、`GraphController`、`RagTraceController`、`UserController`、`AuthController`、`BizChangeLogController`、`DashboardController`、`EvalController` 等 |
| `@RestControllerAdvice` + `@ExceptionHandler` | `framework/web/GlobalExceptionHandler` —— 统一兜底参数校验、业务/未登录/无权限/上传超限异常，返回 `Results.failure(...)`；并用 `@Value` 读取 multipart 限制值生成精确文案 |
| `@Service` / `@Component` | 全部 ServiceImpl 与基础组件（拦截器、装配器、消费者等） |
| `@Primary` | `RoutingLLMService`、`RoutingEmbeddingService`、`RoutingRerankService`、`RoutingVlmService`、`HttpClientConfig` 的 OkHttpClient —— 当同一接口有多个实现/Bean 时指定默认注入项（路由版优先于裸客户端） |
| `@Autowired` | `DelegatingTransactionListener`、`AbstractOpenAIStyleChatClient` 等少量字段注入（项目主体用 Lombok `@RequiredArgsConstructor` 构造注入） |
| `@Value` | 30+ 处配置注入，例如：`KnowledgeDocumentServiceImpl` 的 MQ topic（`@Value("knowledge-document-chunk_topic${unique-name:}")`）、`RAGRateLimitProperties` 的全局并发阈值、`GlobalExceptionHandler` 的上传大小、`RedisKeySerializer` 的 key 前缀、`IdempotentSubmitAspect` 的 eval 开关 |
| `@ConfigurationProperties` | 16 个配置类（详见第 9 节表格） |
| `@Transactional` | 9 个服务类 22 处，集中在 `KnowledgeChunkServiceImpl`（8 处）、`IntentTreeServiceImpl`、`IngestionPipelineServiceImpl`、`KnowledgeDocumentServiceImpl`、`ConversationServiceImpl` 等 —— 分块/入库等多表写操作的事务边界 |
| `@Scheduled` | `knowledge/schedule/KnowledgeDocumentScheduleJob`：`@Scheduled(fixedDelay = 60_000, initialDelay = 30_000)` 与 `@Scheduled(fixedDelayString = "${rag.knowledge.schedule.scan-delay-ms:10000}")` —— 文档定时刷新扫描（配合 Redisson 分布式锁防多实例并发） |
| `@Aspect` + `@Around` | `IdempotentSubmitAspect`、`IdempotentConsumeAspect` —— 自定义幂等注解的织入逻辑 |

### 8.3 MyBatis-Plus 注解

| 注解 | 使用位置与用途 |
|---|---|
| `@TableName("t_xxx")` | 全部 20 个 DO 实体：`KnowledgeBaseDO(t_knowledge_base)`、`KnowledgeDocumentDO(t_knowledge_document)`、`KnowledgeChunkDO(t_knowledge_chunk)`、`ConversationDO(t_conversation)`、`ConversationMessageDO(t_message, autoResultMap = true)`、`IntentNodeDO(t_intent_node)`、`UserDO(t_user)`、`BizChangeLogDO(t_biz_change_log, autoResultMap = true)`、Ingestion 系列 4 张表、Trace 2 张表等。`autoResultMap = true` 是为了让 JSON 字段的 TypeHandler 在查询时也生效 |
| `@TableId(type = IdType.ASSIGN_ID)` | 所有主键 —— 走 `CustomIdentifierGenerator` 雪花算法分配分布式 ID |
| `@TableField(typeHandler = XxxTypeHandler.class)` | JSON/数组字段映射：`BizChangeLogDO` 3 处用 `JsonbTypeHandler`（PostgreSQL jsonb）；`ConversationMessageDO` 用 `SourceRefListTypeHandler`、`GroundingChunkListTypeHandler`、`StringListTypeHandler`；`IngestionTaskDO`/`IngestionPipelineNodeDO`/`KnowledgeDocumentDO` 用 `JsonbTypeHandler`。TypeHandler 定义在 `knowledge/dao/handler/` |
| `@TableLogic` | 所有 DO 的 `deleted` 字段 —— 逻辑删除（配合 `MyMetaObjectHandler` 自动填充） |
| `@MapperScan` | 启动类批量扫描 5 个 mapper 包（详见 3.1） |

### 8.4 RocketMQ 注解

| 注解 | 使用位置与用途 |
|---|---|
| `@RocketMQMessageListener` | 3 个消费者类：`KnowledgeDocumentChunkConsumer`（文档分块主题）、`KnowledgeBaseCleanupConsumer`（知识库清理主题）、`MessageFeedbackConsumer`（消息反馈主题）——声明 topic/consumerGroup，类实现 `RocketMQListener<MessageWrapper<...>>` |
| `@RocketMQTransactionListener` | `framework/mq/producer/DelegatingTransactionListener` —— 注册 RocketMQ 事务消息回查监听，内部按消息 key 委托给业务 `TransactionChecker`（半消息回查：本地事务是否成功） |

### 8.5 操作日志（bizlog-sdk）注解

| 注解 | 使用位置与用途 |
|---|---|
| `@EnableLogRecord(tenant = "ragent", proxyTargetClass = true)` | `RagentApplication` —— 开启操作日志切面 |
| `@LogRecord(...)` | 9 个服务类 30+ 处方法级审计：`UserServiceImpl`（用户增删改密）、`KnowledgeBaseServiceImpl` / `KnowledgeDocumentServiceImpl` / `KnowledgeChunkServiceImpl`（知识库/文档/分块全操作）、`IntentTreeServiceImpl`、`IngestionTaskServiceImpl`、`IngestionPipelineServiceImpl`、`SampleQuestionServiceImpl`、`QueryTermMappingAdminServiceImpl`。日志由 `BizChangeLogRecordService implements ILogRecordService` 落库，操作人由 `RagentOperatorGetService implements IOperatorGetService` 提供 |

### 8.6 Sa-Token 注解 API（非注解式，编程式使用）

项目鉴权采用**拦截器 + 编程式 API** 而非 `@SaCheckLogin` 注解式：`SaTokenConfig` 注册 `SaInterceptor`（登录校验），`UserContextInterceptor` 用 `StpUtil.getLoginId()` 填充 `UserContext`（`AuthServiceImpl` 3 处、`UserController` 4 处、`ChatQueueLimiter` 也直接使用 StpUtil API）。权限数据源由 `SaTokenStpInterfaceImpl implements StpInterface` 提供。

### 8.7 Lombok / Jackson / Validation 注解

| 类别 | 注解 | 使用情况 |
|---|---|---|
| Lombok | `@Data`、`@Getter`、`@Setter`、`@Builder`、`@RequiredArgsConstructor`、`@AllArgsConstructor`、`@NoArgsConstructor`、`@Slf4j` | 全项目实体/DTO/配置类/服务类标配；`@RequiredArgsConstructor` 是主要注入方式（final 字段构造注入） |
| Jackson | `@JsonProperty`、`@JsonValue`、`@JsonInclude` 等 16 处 | `framework/convention/SourceRef`、`GroundingChunk`（引用来源序列化）、`rag/dto/CompletionPayload` 等 SSE 载荷、`core/chunk/ChunkingMode`、`ingestion/domain/enums/*` 枚举序列化 |
| Jakarta Validation | `@NotNull/@NotBlank/@NotEmpty/@Size/@Min/@Max/@Pattern` 10 处（`MemoryProperties`、`RagSemaphoreProperties`）+ Controller 上 `@Valid/@Validated` + 自定义 `@ValidMemoryConfig` | 请求对象与配置类的字段级校验；校验失败由 `GlobalExceptionHandler` 统一转 400 |

---

## 9. 配置体系与 SPI 扩展点

### 9.1 `@ConfigurationProperties` 配置类总表

| 前缀 | 配置类（路径） | 职责 |
|---|---|---|
| `ai` | `infra-ai/.../config/AIModelProperties` | 模型 provider、四类模型候选、档位、超时、熔断 |
| `rag.memory` | `rag/config/MemoryProperties`（挂 `@ValidMemoryConfig`） | 会话记忆窗口/摘要策略 |
| `rag.search` | `rag/config/SearchChannelProperties`（implements `InitializingBean`） | 检索通道开关、topK、召回预算、RRF 权重 |
| `rag.default` | `rag/config/RAGDefaultProperties` | 默认 collection/维度等 |
| `rag.keyword` / `rag.graph` / `rag.guidance` / `rag.trace` / `rag.storage` | `KeywordProperties` / `GraphProperties` / `GuidanceProperties` / `RagTraceProperties` / `RagStorageProperties` | 关键词索引 / 图谱 / 歧义引导 / 追踪 / 存储配置 |
| `rag.rate-limit.*` | `rag/config/RAGRateLimitProperties`（`@Value` 注入） | 全局并发与排队等待 |
| `rag.semaphore` | `knowledge/config/RagSemaphoreProperties` | 分块并发信号量 |
| `rag.knowledge.schedule` | `knowledge/config/KnowledgeScheduleProperties` | 定时刷新扫描间隔等 |
| `rag.mcp` | `rag/core/mcp/McpClientProperties` | MCP Server 地址与超时 |
| `rag.image-parse` / `mineru` | `core/parser/image/ImageParseProperties` / `core/parser/mineru/MinerUProperties` | 图片解析 / MinerU 解析服务 |
| `app` / `app.eval` | `rag/config/DemoModeProperties` / `rag/eval/EvalProperties` | 演示模式 / 评测开关 |

主配置文件 `bootstrap/src/main/resources/application.yaml` 按上述前缀组织：存储类型（s3/oss）、向量库类型（milvus/pgvector）、检索通道开关与 RRF 融合参数、SSE 超时、AI 模型列表等。提示词模板集中在 `resources/prompt/*.st`（StringTemplate，17 个：意图分类、改写、歧义检查、MCP 提参、答案生成各场景）。排队限流 Lua 脚本 `resources/lua/queue_claim_atomic.lua`。

### 9.2 `META-INF/spring.factories` SPI 注册

`bootstrap/src/main/resources/META-INF/spring.factories`：

- `org.springframework.boot.env.EnvironmentPostProcessor` → `RetrievalConfigEnvironmentPostProcessor`（EnvironmentPostProcessor 只能从 spring.factories 加载，在配置绑定前校验检索通道配置矛盾）；
- `org.springframework.boot.diagnostics.FailureAnalyzer` → `RetrievalConfigFailureAnalyzer`（把配置异常渲染成启动失败诊断报告）。

---

## 10. 设计模式与工程亮点总结

| 模式/机制 | 项目中的落地 |
|---|---|
| 接口抽象 + 多实现路由 | AI 四能力（Chat/Embedding/Rerank/VLM）、向量库双实现（Milvus/pgvector）、对象存储双实现（S3/OSS）、解析器 6 实现 |
| 策略模式 | `SearchChannel` 4 通道、`ChunkingStrategy` 2 策略 |
| 责任链 | 检索结果 4 个 `SearchResultPostProcessor`（去重→融合→重排→元数据） |
| 装饰器模式 | `KeywordSyncingVectorStoreService` / `GraphSyncingVectorStoreService` 包裹向量库，写向量自动同步 ES/图谱 |
| 模板方法 | `AbstractParallelRetriever`、`AbstractOpenAIStyleChatClient`（公共请求/SSE 逻辑，子类只填差异） |
| 断路器 | `ModelHealthStore`（CLOSED/OPEN/HALF_OPEN）+ `ModelRoutingExecutor` 自动降级 |
| 首包探测 | `LlmFirstPacketProbe`/`ProbeStreamBridge`：流式响应超时/空首包即切模型 |
| AOP 注解化 | `@IdempotentSubmit`（Redisson 锁）、`@IdempotentConsume`（Redis Lua）、`@RagTraceNode`（Trace 采集）、`@LogRecord`（bizlog） |
| 事务消息 | RocketMQ 半消息 + `TransactionChecker` 回查 + `@IdempotentConsume` 消费幂等，保证"DB 落库 → MQ 消息 → 分块执行"最终一致 |
| BeanPostProcessor 增强 | 启动期自动给向量库 Bean 叠加关键词/图谱同步能力，业务零侵入 |
| 启动期配置校验 | `EnvironmentPostProcessor` + `FailureAnalyzer` + `InitializingBean` + `@ValidMemoryConfig`，配置错误 fail-fast 且诊断友好 |
| 上下文传递 | `UserContext` 基于 TransmittableThreadLocal，MQ 消费者/线程池场景不丢用户身份 |

---

*生成时间：2026-09-20。基于仓库根目录静态源码分析；如后续代码迭代，请以最新源码为准。*
