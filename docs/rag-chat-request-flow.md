# RAG 问答全链路详解：从用户一句话到业务回复

> 本文基于源码逐层拆解「用户发起一次提问 → 前端收到完整回复」的全过程，按**执行顺序**分为 5 大段（接入 → 入口编排 → 流水线 8 阶段 → 流式回写 → 基础设施），每段列出**功能职责、实现代码位置、以及该处用到的小架构**，共归纳 20 个小架构。所有引用均标注类名与文件路径，便于对照源码阅读。

***

## 目录

- [总览链路](#总览链路)
- [一、接入层：HTTP 入口 + 鉴权 + 幂等](#一接入层http-入口--鉴权--幂等)
- [二、入口编排：RAGChatServiceImpl](#二入口编排ragchatserviceimpl)
- [三、流水线：StreamChatPipeline 的 8 个阶段](#三流水线streamchatpipeline-的-8-个阶段)
- [四、LLM 调用与流式回写](#四llm-调用与流式回写)
- [五、支撑基础设施](#五支撑基础设施)
- [六、完整时序（含分支）](#六完整时序含分支)
- [小架构索引](#小架构索引)

***

## 总览链路

```
GET /rag/v3/chat (SSE)
→ 幂等 + 鉴权 + 用户上下文
→ RAGChatServiceImpl.streamChat          入口编排
→ ChatQueueLimiter.enqueue               全局限流排队
→ StreamChatTraceRunner.run              Trace 埋点包裹
→ StreamChatPipeline.execute             8 阶段流水线
   1 loadMemory     2 rewriteQuery       3 resolveIntents
   4 handleGuidance 5 handleSystemOnly   6 retrieve(KB多通道+MCP)
   7 handleEmptyRetrieval                8 streamRagPrompt+LLM
→ RoutingLLMService.streamChat            模型路由/故障转移
→ AbstractOpenAIStyleChatClient.doStreamChat  OkHttp 读 SSE
→ StreamChatEventHandler.onContent/onComplete  回写 SSE + 落库
```

***

## 一、接入层：HTTP 入口 + 鉴权 + 幂等

**实现代码**：`bootstrap/.../rag/controller/RAGChatController.java`

```java
@IdempotentSubmit(key = "T(...UserContext).getUserId()",
                  message = "当前会话处理中，请稍后再发起新的对话")
@GetMapping(value = "/rag/v3/chat", produces = "text/event-stream;charset=UTF-8")
public SseEmitter chat(String question, String conversationId, Boolean deepThinking) {
    SseEmitter emitter = new SseEmitter(ragDefaultProperties.getSseTimeoutMs());
    ragChatService.streamChat(question, conversationId, deepThinking, emitter);
    return emitter;   // 立即返回 emitter，业务在别的线程跑
}
```

相关：停止任务 `POST /rag/v3/stop` → `StreamTaskManager.cancel(taskId)`。

### 小架构 1｜AOP + 注解 + SpEL 幂等

`framework/.../idempotent/IdempotentSubmitAspect.java` 环绕通知，用 `SpELUtil` 解析 `key` 表达式（这里是 userId），在 Redis 上抢占式加锁：同一用户已有进行中的流式任务就直接拒绝，避免并发打爆模型。

### 小架构 2｜拦截器 + ThreadLocal 用户上下文

`bootstrap/.../user/config/SaTokenConfig.java` 注册 `UserContextInterceptor`，把登录态写入 `framework/.../context/UserContext.java`。

**关键点**：它是 `TransmittableThreadLocal` 语义，配套线程池全部用 `TtlExecutors.getTtlExecutor()` 包装（见第五节），否则异步检索线程里取不到 userId、会话落库会失败。

***

## 二、入口编排：RAGChatServiceImpl

**实现代码**：`bootstrap/.../rag/service/impl/RAGChatServiceImpl.java`

```java
String actualConversationId = StrUtil.isBlank(conversationId)
        ? IdUtil.getSnowflakeNextIdStr() : conversationId;   // 无会话则新建雪花ID
String taskId = IdUtil.getSnowflakeNextIdStr();               // 任务ID（取消用）
StreamCallback callback = callbackFactory.createChatEventHandler(emitter, actualConversationId, taskId);

chatQueueLimiter.enqueue(question, actualConversationId, emitter,
    () -> traceRunner.run(question, actualConversationId, taskId, callback, traceAware -> {
        StreamChatContext ctx = StreamChatContext.builder()
                .question(question).conversationId(actualConversationId).taskId(taskId)
                .deepThinking(...).userId(UserContext.getUserId()).callback(traceAware)
                .build();
        chatPipeline.execute(ctx);
    }));
```

这一层是**门面（Facade）+ 三层包装**：限流在外、Trace 在中、流水线在内。它自己几乎不含业务逻辑，只做 ID 生成、上下文装配、包装顺序编排。

### 小架构 3｜流式限流排队

`bootstrap/.../rag/service/ratelimit/ChatQueueLimiter.java`

- 走 `FairDistributedRateLimiter`（Redis 公平分布式限流），参数 `maxWaitMillis`；`onAcquired` 才真正执行，`onTimeout` 走拒绝分支。
- `cancelBinder` 把 `emitter.onCompletion/onTimeout/onError` 绑到限流令牌释放上——**这是防泄漏的关键**：前端断连必须释放并发额度，否则额度被永久占用。
- 拒绝分支 `handleReject`：仍然把用户问题 + 一条 `REJECTED` 状态的助手消息落库，然后下发 `meta → reject → finish → done` 四个事件，保证前端协议一致（不会卡住）。

### 小架构 4｜Trace 包装器（装饰器）

`bootstrap/.../rag/trace/StreamChatTraceRunner.java`

开启 trace 时创建 `RagTraceRunDO`（traceId/taskId/entryMethod），并用 `ForwardingStreamCallback` 包装业务 callback，在**不侵入业务代码**的前提下截获两个时机：

- `onFirstContent()` → 写 `user-first-packet` 节点，即**用户感知 TTFT**（路由 + 改写 + 意图 + 检索 + 模型首包全链路耗时）；
- `onFinish()` → `finishRun` 收尾。

最后 `finally` 里清理 `RagTraceContext` ThreadLocal，防止线程池复用污染。

***

## 三、流水线：StreamChatPipeline 的 8 个阶段

**实现代码**：`bootstrap/.../rag/service/pipeline/StreamChatPipeline.java`

```java
loadMemory(ctx);  rewriteQuery(ctx);  resolveIntents(ctx);
if (handleGuidance(ctx))    return;      // 歧义追问，短路
if (handleSystemOnly(ctx))  return;      // 纯系统意图，短路
RetrievalContext retrievalCtx = retrieve(ctx);
if (handleEmptyRetrieval(ctx, retrievalCtx)) return;  // 空召回，短路
streamRagResponse(ctx, retrievalCtx);
```

### 小架构 5｜"返回 boolean 即短路"的流水线模式

每个 `handleXxx` 返回 `true` 表示"已处理并终止"，避免了嵌套 if-else，也让三条旁路（追问 / 系统直答 / 空召回）与主路径平级。这是典型的**管道-过滤器 + 短路语义**，比责任链更轻量（无 handler 链表，编译期可见顺序）。

上下文载体：`StreamChatContext`（question / conversationId / taskId / deepThinking / userId / history / rewriteResult / subIntents / callback）。

### 阶段 1：loadMemory —— 记忆加载 + 用户消息落库

`StreamChatPipeline#loadMemory` → `DefaultConversationMemoryService`

- `load()`：**并行**拉「摘要」和「历史」两个 future（`memoryLoadExecutor`），`CompletableFuture.allOf` 汇合后把摘要 `decorateIfNeeded` 后插到 history 首位 → 摘要天然成为第一条 system 消息。
- `append()`：写用户消息 → 返回 `questionMessageId` → `callback.onReplyToMessageId(...)`，建立问答消息引用关系（前端"引用追问"用）。
- 底层 `JdbcConversationMemoryStore`：写库同时触发 `ConversationService.createOrUpdate` 建会话，**首次提问会触发 `ConversationTitleGenerator` 用 LLM 生成标题**（失败兜底"新对话"）。
- 摘要压缩 `JdbcConversationMemorySummaryService`：只在助手消息落库时**异步**判断（`summaryStartTurns` / `historyKeepTurns` 阈值），达到阈值才把老消息 LLM 压缩成 `ConversationSummaryDO`，失败回退旧摘要。

#### 小架构 6｜并行聚合 + 缓存旁路 + 写时副作用

摘要 / 历史两路独立 future，任一路异常只降级自己（`loadSummaryWithFallback` / `loadHistoryWithFallback`），不让记忆加载拖垮主链路。

### 阶段 2：rewriteQuery —— 术语归一化 + 改写 + 多问句拆分

`StreamChatPipeline#rewriteQuery` → `MultiQuestionRewriteService#rewriteWithSplit`

两条分支：

1. `queryRewriteEnabled=false` → 只做 `queryTermMappingService.normalize()`（DB 术语映射表 + `QueryTermMappingCacheManager` 按 key 缓存）+ 规则拆分 `ruleBasedSplit`（按 `?？。；;\n` 切）。
2. 开启 → LLM 改写 `callLLMRewriteAndSplit`：
   - 只带最近 **4 条** User/Assistant 历史（显式过滤掉 system 摘要，省 token）；
   - `temperature=0.1 / topP=0.3`，走 **FAST 档**模型；
   - 要求输出 JSON `{"rewrite": "...", "sub_questions": [...]}`；
   - `LLMResponseCleaner.stripMarkdownCodeFence` 去围栏 → Gson 解析 → `RewriteResult(rewrittenQuestion, subQuestions)`；
   - **任何解析 / 调用失败都兜底为"归一化后原问题 + 单子问题"**。

#### 小架构 7｜结构化输出 + 容错降级（Fallback）

LLM 输出是不可信输入，这里统一做了「去围栏 → JSON 校验 → 字段校验 → 兜底」四步；同类模式在后面意图识别、MCP 提参处重复出现。

### 阶段 3：resolveIntents —— 意图树分类（并行打分）

`StreamChatPipeline#resolveIntents` → `IntentResolver#resolve`

- 每个子问题一个 `CompletableFuture`（`intentClassifyExecutor`）**并行**分类，异常降级为空意图（不影响其他子问题）。
- `DefaultIntentClassifier#classifyTargets`：
  - 意图树先读 Redis（`IntentTreeCacheManager`），未命中才查 DB（`intent_node` 表 `deleted=0 & enabled=1`）并回写缓存；
  - 扁平列表两次遍历组装父子树 + `fillFullPath` 生成 `业务系统 > OA系统 > 系统介绍`；
  - `buildPrompt` 把**全部叶子节点**的 id / path / description / type / examples 塞进 prompt，让 LLM 只在这些 id 上打分，输出 `[{"id","score"}]`；
  - 解析后降序排序；调用失败或 JSON 畸形 → 空意图（下游当"无意图"兜底走全局检索）。
- 再过滤 `score >= INTENT_MIN_SCORE` 且 `limit MAX_INTENT_COUNT`。
- `capTotalIntents`：多子问题导致意图总数超限时的**配额裁剪算法**——每个子问题先保底保留 1 个最高分意图，剩余配额按全局分数从高到低分配。

#### 小架构 8｜Cache-Aside 树形分类器 + 配额分配

意图树是"读多写少"的配置数据，Redis 做一层旁路缓存；分类是"一次 LLM 调用覆盖全部叶子"，靠 prompt 而非多次调用来控成本。

### 阶段 4：handleGuidance —— 歧义追问（第 1 条短路）

`StreamChatPipeline#handleGuidance` → `IntentGuidanceService#detectAmbiguity`

判定依据（`GuidanceDecision`）：

- top1 与 top2 的**分数比值 / 分差**低于 `ambiguityScoreRatio` / `ambiguityMargin` → 判为模糊；
- 落在阈值**边界带**时，再用 `AmbiguityLLMChecker` 让 LLM 复核一次；
- 问题里**已明确命中系统名**时跳过澄清（用户说得很清楚就别多问）。

命中 → 直接把追问话术 `onContent` + `onComplete` 结束，不进检索。

#### 小架构 9｜规则 + LLM 二级判定

规则快（0 成本）但边界不准，LLM 准但贵；只在边界带调 LLM，兼顾成本与准确率。这是"先用便宜信号筛，再用贵模型裁决"的典型分层决策。

### 阶段 5：handleSystemOnly —— 纯系统意图直答（第 2 条短路）

`StreamChatPipeline#handleSystemOnly`

所有子问题意图都满足 `isSystemOnly`（即只有 1 个且 `kind=SYSTEM`）→ 不检索，直接取该节点 `promptTemplate`（空则默认 `CHAT_SYSTEM_PROMPT_PATH`）调 LLM。典型场景："你是谁 / 你能做什么"。

#### 小架构 10｜意图驱动的三态路由

`IntentKind` 有 **KB / MCP / SYSTEM** 三值，决定了后续走向：SYSTEM → 直答短路；KB → 多通道检索；MCP → 工具调用。这是整个链路的"第一级分流器"。

### 阶段 6：retrieve —— 检索（最重的一段）

`StreamChatPipeline#retrieve` → `RetrievalEngine#retrieve`

先算一次**全局预算**（所有子问题共用）：

```java
RetrievalBudget budget = new RetrievalBudget(
        searchProperties.resolveRecallBudget(contextTopK),      // 召回扇出
        searchProperties.getFusion().getRerankCandidateLimit(), // Rerank 候选池上限
        searchProperties.getDefaultTopK());                     // 最终条数
```

再**并行**（`ragContextExecutor`）为每个子问题构建上下文 `buildSubQuestionContext`：KB 意图 → `retrieveAndRerank`，MCP 意图 → `executeMcpAndMerge`。

多子问题时用模板 `sub-question-kb-wrapper` / `sub-question-mcp-wrapper` 分段拼接成 `kbContext` / `mcpContext`；最终产出 `RetrievalContext(mcpContext, kbContext, intentChunks)`。

#### 小架构 11｜Worker 并行 + 结果聚合

三层嵌套并行：子问题级（`ragContextExecutor`）→ 通道级（`ragRetrievalExecutor`）→ 库级（`innerRetrievalExecutor`）。每层都用 `CompletableFuture` + 单个任务 try-catch 降级，是"局部失败不放大为全链路失败"的隔离设计。

#### 6a. KB：多通道检索 + 后置处理器链

`MultiChannelRetrievalEngine#retrieveKnowledgeChannels`

**阶段 1：通道并行**（按 `type.ordinal()` 排序仅为日志稳定，不承载优先级）

| 通道     | 实现类                   | 开关                                            | 说明        |
| ------ | --------------------- | --------------------------------------------- | --------- |
| Vector | `VectorSearchChannel` | `rag.search.channels.vector.enabled`          | 核心通道      |
| Keyword | `KeywordSearchChannel` | `rag.keyword.type=es`（`@ConditionalOnProperty`） | ES BM25   |
| Graph  | `GraphSearchChannel`  | `rag.graph.type=lightrag`                     | 多跳关系，过滤时 topK×3 补召回 |
| Web    | `WebSearchChannel`    | enabled + 有 API Key                           | You.com，**任何异常都降级空结果** |

向量通道内部还有一层**作用域二选一**（`shouldNarrowToIntent`）：

- 有 KB 意图且 `maxScore >= confidenceThreshold`（且单意图时不低于 `singleIntentSupplementThreshold`）→ **意图定向**：`IntentParallelRetriever.retrieveByIntents` 并行查命中的 collections，`node.topK` 可覆盖该意图的绝对深度；
- 否则 → **全局兜底**：PG 后端走 `retrieveGlobal` 单条 SQL 带总预算，其他后端退化为 `CollectionParallelRetriever` 逐库 fan-out。

**阶段 2：后置处理器链**（按 `getOrder()` 串行，单个失败只记日志跳过）

| Order | 处理器                        | 启用条件                    | 核心逻辑                                                       |
| ----- | -------------------------- | ----------------------- | ---------------------------------------------------------- |
| 1     | `DeduplicationPostProcessor` | 始终                      | 按 `ChannelAttribution.keyOf`（优先 id，缺失用文本 SHA-256）去重，保留首次出现 |
| 5     | `FusionPostProcessor`        | `fusion.strategy=rrf`   | RRF：`score = Σ weight / (k + rank + 1)`，**仅多通道才融合**；再按 `rerankCandidateLimit` 截断候选池 + 输出通道归因日志 |
| 10    | `RerankPostProcessor`        | `rerankEnabled`         | `RerankService.rerank(mainQuestion, chunks, contextTopK)` 精排 |
| 20    | `MetadataEnrichmentPostProcessor` | —                  | Rerank 之后按 chunkId 回表补 `docId/chunkIndex/docName`，图谱证据按 docId 补标题 |

融合后 `RetrievalEngine#retrieveAndRerank` 按意图节点 ID 把 chunks 分组进 `intentChunks`，再由 `ContextFormatter.formatKbContext(kbIntents, intentChunks, contextTopK)` 格式化成给 LLM 的文本。

##### 小架构 12｜通道-插件 + 处理链（Chain of Responsibility with Order）

`SearchChannel` / `SearchResultPostProcessor` 两个扩展点都靠 Spring 注入 `List<T>` 自动收集实现，新增通道 / 处理器只需加一个 `@Component` + 实现 `isEnabled/getOrder`，引擎零改动。RRF 的两个关键设计：**只用名次不用原始分**（跨模态分数不可比）、**截断候选池**（Rerank 是花钱的，先粗排后精排控制成本）。

#### 6b. MCP：工具调用（含三态分流）

`RetrievalEngine#executeMcpTools`：每个 mcp 意图并行（`mcpBatchExecutor`）→ `mcpToolRegistry.getExecutor(toolId)` → `mcpParameterExtractor.extractParameters(question, tool, customParamPrompt)`。

**参数提取三态**（`LLMMcpParameterExtractor` / `McpExtractionResult`）：

| 状态                   | 判定         | 处理                                                         |
| -------------------- | ---------- | ---------------------------------------------------------- |
| `SUCCESS`            | JSON 合法且必填齐全 | `executor.execute(params)` 真调远端工具                         |
| `NEED_CLARIFICATION` | 必填参数缺失     | **不调用**，注入 note（`isError=false`）让 LLM 主动向用户追问              |
| `FAILED`             | 协议畸形 / 值非法 | **不调用**，注入提示（`isError=true`）进"工具调用失败"段                      |

工具发现与执行：`McpClientAutoConfiguration` 启动时按 `rag.mcp.servers` 连接每个 MCP Server，`listTools()` 发现工具并为每个工具注册 `McpClientToolExecutor` 到 `DefaultMcpToolRegistry`，运行时 `McpSyncClient.callTool(...)`。Server 端（`mcp-server` 模块）如 `WeatherMcpExecutor` 用手写 `JsonSchema` 定义 `weather_query` 的 city / queryType / days。

##### 小架构 13｜工具注册表 + 三态状态机

"发现-注册-按 id 查找-执行"是标准注册表模式，工具对主流程完全透明。三态设计的精髓在于**用 `isError` 标志区分"正文注入"和"失败注入"**——缺参数不是错误，而是让 LLM 学会追问，避免编造参数。

### 阶段 7：handleEmptyRetrieval —— 空召回短路

`retrievalCtx.isEmpty()` → 直接回"未检索到与问题相关的文档内容。" + `onComplete`，**不调 LLM**（省一次模型调用）。

### 阶段 8：streamRagResponse —— Prompt 组装 + 流式输出

`StreamChatPipeline#streamRagResponse`

1. `intentResolver.mergeIntentGroup(subIntents)` → `IntentGroup(mcpIntents, kbIntents)`；
2. `sourcesAssembler.assemble(intentChunks)` → `callback.onSources(sources)`：按 docId 去重保留最高分，限 `MAX_SOURCES=20`，产出 `SourceRef`（**仅面板 / 预览用，不进 prompt**）；
3. `groundingChunksAssembler.assemble(...)` → 限 `MAX_CHUNKS=8` 的 `GroundingChunk`，**随助手消息落库**，供后续"推荐追问"生成 grounding；
4. `streamLLMResponse`：组装 `PromptContext` → `RAGPromptService#buildStructuredMessages` 生成消息序列；参数随场景变化：`temperature = MCP?0.3:0`、`topP = MCP?0.8:1`、`thinking = deepThinking`。

#### Prompt 组装的小架构（模板 + 场景规划）

```
[system]  ← plan() 按场景选模板
[history] ← 摘要(system) + 历史消息，原样透传
[user]    ← 证据段(mcp-evidence / kb-evidence) + 问题段(single-question / multi-questions)
```

模板选择 `plan()` 按 `hasMcp / hasKb` 四象限路由：

- MCP only → `planMcpOnly`（单 MCP 意图且有节点模板时用节点模板，否则 `MCP_ONLY_PROMPT_PATH`）；
- KB only → `planPrompt`：**先剔除"未命中检索"的意图**，单意图且有 `promptTemplate` 就用节点自带模板，多意图统一 `RAG_ENTERPRISE_PROMPT_PATH`；
- Mixed → `MCP_KB_MIXED_PROMPT_PATH`；
- 都没有 → 抛 `IllegalStateException`（阶段 7 已挡住）。

##### 小架构 14｜场景规划器（Planner）+ 片段渲染

`plan → build → render` 三分离；模板不是拼接字符串，而是通过 `PromptTemplateLoader` 的 `render/renderSection` 按具名 section（`kb-evidence`、`multi-questions`…）取值，模板可运维化改文案而不动代码。

***

## 四、LLM 调用与流式回写

### 4.1 模型路由与故障转移

`infra-ai/.../chat/RoutingLLMService#streamChat`

```
targets = selector.selectChatCandidates(thinking)
for target in targets:
    client = clientsByProvider[target.provider]
    if !healthStore.allowCall(target.id) continue        # 健康度熔断
    handle = client.streamChat(request, bridge, target)
    result = firstPacketProbe.awaitFirstPacket(bridge, timeout)  # 首包探测
    if result.isSuccess(): healthStore.markSuccess(); return handle
    else: healthStore.markFailure(); handle.cancel(); next   # 换下一个模型
throw notifyAllFailed(callback, lastError)
```

四种失败判定：`ERROR`（启动异常 / 返回 null handle）/ `TIMEOUT`（首包超时）/ `NO_CONTENT`（无内容即完成）/ 异常中断。

#### 小架构 15｜路由器 + 健康度 + 首包探针

- **健康度熔断**：`ModelHealthStore` 记录每个 modelId 的失败情况，被判定不健康的直接跳过，避免反复打到坏节点；
- **首包探针**：`ProbeStreamBridge` 缓冲首个 token，`LlmFirstPacketProbe` 在预算内等首包；`isSuccess()` 之后才把已缓冲内容转发给真实 callback。**好处**：模型 A 首包超时，用户根本感知不到，直接换 B 重试，只有整段内容还没吐给前端时才能无感切换——这是"可回滚的故障转移"。

### 4.2 HTTP 流式读取（模板方法）

`infra-ai/.../chat/AbstractOpenAIStyleChatClient#doStreamChat`

- 子类只实现 `provider()` / `customizeRequestBody()`（如百炼的 `enable_thinking`），父类统管 `doChat`（同步）与 `doStreamChat`（异步 SSE）；
- OkHttp `streamingHttpClient` 发请求，`StreamAsyncExecutor.submit(modelStreamExecutor, call, callback, task)` 抛到线程池；
- 循环 `source.readUtf8Line()` → `OpenAIStyleSseParser.parseLine` → `onThinking()`（reasoning_content）/ `onContent()` / `onComplete()`；
- `AtomicBoolean cancelled` 支持中途停止；`StreamSpanCallback` 把流式跨度的真实端到端耗时记进 trace。

#### 小架构 16｜模板方法 + 供应商适配器

4 个 provider 客户端（SiliconFlow / BaiLian / Ollama / AIHubMix）继承同一个抽象基类，差异全部下沉到钩子方法，是典型的"父类定骨架、子类填差异"。

### 4.3 回写 SSE 与落库

`bootstrap/.../rag/service/handler/StreamChatEventHandler`（`StreamCallback` 的实现）

| 回调                          | 行为                                                                                                                            |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| 构造                          | 发 `META(conversationId, taskId)` + `taskManager.register(taskId, sender, cancelPayloadSupplier)`                                 |
| `onReplyToMessageId`        | 记住问题消息 id                                                                                                                      |
| `onSources` / `onGroundingChunks` | **只暂存**，不立即下发（随 FINISH 一起给，避免前端拿到无用的中间态）                                                                                       |
| `onThinking`                | 累积 thinking + 记录首字时间；`sendChunked(TYPE_THINK, …)`                                                                              |
| `onContent`                 | 首次进正文时结算 thinking 耗时；累积 answer；`sendChunked` 按 `messageChunkSize`（默认 5）**按码点切分**（防 emoji 截断）发 `MESSAGE(MessageDelta(type, content))` |
| `onComplete`                | 组装 `ChatMessage.assistant(content, thinking, duration)` + sources + retrievedChunks + replyToMessageId + `NORMAL` → `memoryService.append` 落库 → 发 `FINISH(CompletionPayload)` + `DONE("[DONE]")` → `unregister` → `sender.complete()` |
| `onError`                   | `unregister` + `sender.fail(t)`                                                                                                 |
| 取消路径                        | `buildCompletionPayloadOnCancel`：把已累积内容以 `INTERRUPTED` 状态落库，再发 `CANCEL` + `DONE`                                                  |

事件类型见 `SSEEventType`：`meta / message / finish / done / cancel / reject`。

发送器 `framework/.../web/SseEmitterSender`：用 `AtomicBoolean.closed` + CAS 保证 `complete()` / `fail()` **幂等**（只关一次），发送异常内部吞掉转 `fail`，避免流已开始后再抛异常触发全局异常处理器导致响应冲突。

#### 小架构 17｜Callback 观察者 + 幂等资源释放

`StreamCallback` 是整条链路的"输出总线"，业务层只依赖这个接口，不感知 SSE。三处幂等保护：`SseEmitterSender.closed` CAS、`StreamTaskManager.cancelled` CAS、`taskManager.isCancelled()` 前置校验（取消后所有回调直接 return）。

### 4.4 任务取消（跨节点）

`bootstrap/.../rag/service/handler/StreamTaskManager`：本地 Guava Cache（TTL 30min，上限 1w）+ Redis 标记 + Redisson Topic 广播。

`register` 时先查 Redis 是否已被取消（**防止"取消先到、任务后到"的竞态**）；`bindHandle` 时若已取消则立刻 `handle.cancel()`；`cancel(taskId)` 先写 Redis 标记再 publish，保证多节点都能感知。

#### 小架构 18｜本地缓存 + 分布式广播的取消语义

任务句柄只在产生它的 JVM 内存里，所以不能直接远程 cancel——必须"Redis 落标记（权威状态）+ Topic 广播（触发本地句柄 cancel）"双写，且本地节点也走监听器统一处理以避免重复取消。

***

## 五、支撑基础设施

### 5.1 线程池隔离（9 个）

`bootstrap/.../rag/config/ThreadPoolExecutorConfig.java`

| Bean                       | 核心 / 最大                     | 队列            | 拒绝策略       | 用途                              |
| -------------------------- | --------------------------- | ------------- | ---------- | ------------------------------- |
| `chatEntryExecutor`        | `globalMaxConcurrent` 固定    | Synchronous   | Abort      | 限流放行后的执行入口（**并发上限的物理实现**）       |
| `modelStreamExecutor`      | CPU/2 ~ CPU                 | LJB(200)      | Abort      | 模型 SSE 读取（长连接、易阻塞）              |
| `ragContextExecutor`       | CPU×4                       | Synchronous   | CallerRuns | 子问题级并行（检索 + MCP）                |
| `ragRetrievalExecutor`     | CPU×4                       | Synchronous   | CallerRuns | 通道级并行                           |
| `innerRetrievalExecutor`   | CPU×2 ~ CPU×4               | LJB(100)      | CallerRuns | 库级并行 fan-out                    |
| `intentClassifyExecutor`   | CPU ~ CPU×2                 | Synchronous   | CallerRuns | 子问题意图分类并行                       |
| `mcpBatchExecutor`         | CPU ~ CPU×2                 | Synchronous   | CallerRuns | MCP 工具批量调用                      |
| `memoryLoadExecutor`       | CPU/2 ~ CPU                 | LJB(200)      | CallerRuns | 摘要 + 历史并行加载                     |
| `memorySummaryExecutor`    | 1 ~ CPU/2                   | LJB(200)      | CallerRuns | 摘要压缩（后台、低优先）                    |

#### 小架构 19｜舱壁隔离（Bulkhead）+ TTL 透传

- 分池的意义：模型流式读取是**长阻塞**任务，如果和检索用同一个池，10 个慢请求就能把检索线程全占满 → 全站雪崩。分池后各业务域的排队互不影响。
- 检索 / 意图类用 `CallerRunsPolicy`：池满时由调用线程自己跑，**降级为串行而不是丢任务**；入口和模型流用 `AbortPolicy`：宁可快速失败也不能无限堆积。
- 全部经 `TtlExecutors.getTtlExecutor()` 包装，保证 `UserContext` / `RagTraceContext` 跨线程传递（这是全链路 userId、traceId 不断链的前提）。

### 5.2 链路追踪

- 注解埋点：`framework/.../trace/RagTraceNode` + `bootstrap/.../rag/aop/RagTraceAspect`，标在 `MultiQuestionRewriteService#rewriteWithSplit`、`IntentResolver#resolve`、`RetrievalEngine#retrieve`、`MultiChannelRetrievalEngine#retrieveKnowledgeChannels`、`RoutingLLMService#streamChat` 等方法上，落到 `rag_trace_run` / `rag_trace_node` 表；
- 流式补充：`RagStreamTraceSupport` 用线程内 span 栈维护父子关系，`StreamSpanCallback` 收尾节点。

#### 小架构 20｜AOP 埋点 + 上下文传递

业务方法只加一个注解，切面负责 `startNode/finishNode`；`RagTraceContext` 用 ThreadLocal 存 traceId/taskId 与节点栈，异步线程靠 TTL 拿快照副本。

### 5.3 数据落库与消息

- 会话 / 消息：`ConversationDO` / `ConversationMessageDO`（含 sources、retrievedChunks、replyToMessageId、messageStatus）/ `ConversationSummaryDO`；
- 反馈：`MessageFeedbackServiceImpl` + `MessageFeedbackConsumer` 走 RocketMQ，配套 `IdempotentConsume` 幂等消费与 `DelegatingTransactionListener` 事务消息。

***

## 六、完整时序（含分支）

```
用户提问
 ├─ 幂等校验（同用户并发拒绝）
 ├─ 鉴权 → UserContext
 ├─ 限流排队 ─ 超时 ─→ 落库(REJECTED) + meta/reject/finish/done
 ├─ Trace startRun
 ├─ 记忆加载（并行：摘要 + 历史）→ append 用户消息（触发建会话/生成标题）
 ├─ 改写+拆分（术语归一化 → FAST 档 LLM → JSON）
 ├─ 意图分类（意图树 Redis/DB → LLM 打分 → 阈值过滤 → 配额裁剪）
 ├─ 歧义？─→ 追问 + 结束
 ├─ 全 SYSTEM？─→ 系统 Prompt 直答 + 结束
 ├─ 检索（子问题并行）
 │    ├─ KB：多通道并行（向量/关键词/图谱/联网）→ 去重 → RRF → 截断 → Rerank → 元数据回填
 │    └─ MCP：并行提参 → SUCCESS 调用 / 缺参追问注入 / 失败注入
 ├─ 空召回？─→ 固定话术 + 结束
 ├─ 来源装配 onSources / grounding onGroundingChunks
 ├─ Prompt 组装（场景选模板：KB/MCP/Mixed + 证据段 + 问题段）
 ├─ 模型路由（健康度筛选 → 首包探测 → 失败换模型）
 ├─ OkHttp 读 SSE → onThinking/onContent → 按 5 字切分下发 message 事件
 └─ onComplete：助手消息落库(NORMAL) → finish + done → 关闭 emitter
     （中途停止：Redis 标记 + Topic 广播 → handle.cancel → 落库 INTERRUPTED → cancel + done）
```

***

## 小架构索引

| #   | 小架构                              | 所在阶段            |
| --- | -------------------------------- | --------------- |
| 1   | AOP + 注解 + SpEL 幂等                | 接入层             |
| 2   | 拦截器 + ThreadLocal 用户上下文           | 接入层             |
| 3   | 流式限流排队                           | 入口编排            |
| 4   | Trace 包装器（装饰器）                    | 入口编排            |
| 5   | "返回 boolean 即短路"的流水线模式            | 流水线             |
| 6   | 并行聚合 + 缓存旁路 + 写时副作用               | 阶段 1 记忆         |
| 7   | 结构化输出 + 容错降级（Fallback）            | 阶段 2 改写         |
| 8   | Cache-Aside 树形分类器 + 配额分配          | 阶段 3 意图         |
| 9   | 规则 + LLM 二级判定                     | 阶段 4 歧义追问       |
| 10  | 意图驱动的三态路由                         | 阶段 5 系统直答       |
| 11  | Worker 并行 + 结果聚合                 | 阶段 6 检索         |
| 12  | 通道-插件 + 处理链（Order 责任链）            | 阶段 6a KB        |
| 13  | 工具注册表 + 三态状态机                     | 阶段 6b MCP       |
| 14  | 场景规划器（Planner）+ 片段渲染              | 阶段 8 Prompt     |
| 15  | 路由器 + 健康度 + 首包探针                 | LLM 调用          |
| 16  | 模板方法 + 供应商适配器                     | LLM 调用          |
| 17  | Callback 观察者 + 幂等资源释放             | 流式回写            |
| 18  | 本地缓存 + 分布式广播的取消语义                | 任务取消            |
| 19  | 舱壁隔离（Bulkhead）+ TTL 透传            | 基础设施            |
| 20  | AOP 埋点 + 上下文传递                   | 基础设施            |
