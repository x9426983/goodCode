# 图策略（DAG）调试系统 — 完整分析

> 文档基于 wtdebugservice 源码分析，覆盖图策略的读取、调用、执行全流程。

---

## 一、系统全景：两个平面

整个图策略调试系统分为**两个独立的平面**，通过 Mafka 消息队列异步解耦：

```
┌─────────────────────────────────────────────────────────────────────┐
│  平面 A：调试触发平面（wtdebugservice 主动发起）                     │
│                                                                     │
│  前端请求 → Controller → AbstractDebugService → 目标推荐服务         │
│                         ↑ 注入 MTrace Context（图策略Key/调试等级）  │
└─────────────────────────────────────────────────────────────────────┘
                              ↓  目标服务内部执行图策略
                              ↓  SDK 采集执行信息
                              ↓  发送 Mafka 消息
┌─────────────────────────────────────────────────────────────────────┐
│  平面 B：数据接收平面（wtdebugservice 被动消费）                     │
│                                                                     │
│  Mafka topic → Consumer → 反序列化 → 压缩存储 → DB                  │
│                           GraphDebugRequestVO / OperatorVO           │
└─────────────────────────────────────────────────────────────────────┘
```

两个平面共享同一个 `traceId`（由 MTrace 生成），这是关联所有调试数据的唯一键。

---

## 二、图策略读取：如何确定「执行哪张图」

### 2.1 图策略 Key 的注入机制

图策略 Key **不是通过 RPC 参数传递**给下游服务的，而是通过 **MTrace 分布式追踪上下文**透传：

```java
// AbstractDebugService.beforeRequest()
// 源码路径：debugservice-server/.../service/impl/AbstractDebugService.java

Tracer.putContext(
    "worldtree_" + appkey,   // Key = "worldtree_com.sankuai.wmdrecsys.rec.hub"
    params.getHubGraphKey()  // Value = "rec_hub_feed_v2"（前端传入的图策略名）
);
```

**MTrace Context Key 命名规则：**

| Context Key                      | 含义                                   | 示例值                             |
|----------------------------------|----------------------------------------|------------------------------------|
| `worldtreeDebugLevel`            | 调试等级（0-3）                        | `"2"`                              |
| `worldtree_{appkey}`             | 指定该服务使用哪张图策略               | `"rec_hub_feed_v2"`                |
| `worldtreeSendOpDebug_{appkey}`  | 是否上报算子输入输出                   | `"true"`                           |
| `worldtreeSingleServer`          | 单服务模式（只调试链路中的一个节点）   | `"com.sankuai.wmdrecsys.rec.hub"`  |

MTrace Context 会随着 Thrift RPC 调用链自动透传到下游所有服务，下游服务的 WorldTree SDK 从中读取上述配置，决定是否启用调试以及使用哪张图。

### 2.2 多服务全链路场景下的路由注入

当一次调试涉及 RecHub → River → Libra 全链路时，`beforeRequest` 先查询 **Lion 配置中心**，获取 scene 对应的调用链拓扑，再分别为每个下游服务注入各自的图策略 Key：

```java
// 单服务模式：只注入当前 appkey
if (params.getIsSingleService()) {
    Tracer.putContext("worldtree_" + params.getAppkey(), params.getHubGraphKey());
    Tracer.putContext("worldtreeSendOpDebug_" + params.getAppkey(), "true");
    Tracer.putContext("worldtreeSingleServer", params.getAppkey());
} else {
    // 全链路模式：从 Lion 读取 scene 对应的调用链拓扑
    RouteInfoVo.InvokeChainInfo chain = RouteService.routeChain(params.getScene());

    // 为召回层（River）注入
    for (InvokeSceneInfo recallInfo : chain.getRecall()) {
        Tracer.putContext("worldtree_" + recallInfo.getAppKey(), params.getRiverGraphKey());
        Tracer.putContext("worldtreeSendOpDebug_" + recallInfo.getAppKey(), params.getRiverSendOpEnable());
    }

    // 为排序层（Libra）注入
    for (InvokeSceneInfo rankInfo : chain.getRank()) {
        Tracer.putContext("worldtree_" + rankInfo.getAppKey(), params.getLibraGraphKey());
        Tracer.putContext("worldtreeSendOpDebug_" + rankInfo.getAppKey(), params.getLibraSendOpEnable());
    }
}
```

**Lion 配置的调用链拓扑示意：**
```
scene: "feed_shop"
  ├─ recall: [{ appKey: "com.sankuai.wmdrecsys.river", scene: "feed" }]
  └─ rank:   [{ appKey: "com.sankuai.wmdrecsys.libra.rankserver", scene: "feed" }]
```

### 2.3 下游服务如何读取图策略 Key（SDK 侧）

下游推荐服务通过 `DebugGlobalContext` 从 MTrace Context 读取配置：

```java
// debugservice-sdk/.../DebugGlobalContext.java

// 读取调试等级
public static int getCurrentRequestDebugLevel() {
    String debugLevel = Tracer.getContext("worldtreeDebugLevel");
    return StringUtils.isBlank(debugLevel) ? 0 : Integer.parseInt(debugLevel);
}

// 自动准入逻辑（非调试平台发起的请求）
public static void admittanceProcess() {
    if (!debugEnable) {
        // 业务未开启 debug → Level 0 不上报
        Tracer.putContext("worldtreeDebugLevel", "0");
        return;
    }
    if (Tracer.getContext("worldtreeDebugLevel") != null) {
        // 已有等级（调试平台发起）→ 直接使用，不覆盖
        return;
    }
    // 命中用户 ID 白名单 → 自动设置 Level 1（轻量采集）
    if (DebugConfigUtils.isHitUserIdWhiteList(getUserId())) {
        Tracer.putContext("worldtreeDebugLevel", "1");
    }
}
```

---

## 三、图策略调用：触发执行的完整流程

### 3.1 服务注册与路由表（WorldtreeDebugService）

`WorldtreeDebugService` 在首次调用时（懒加载）扫描指定包路径下所有 `AbstractDebugService` 实现，按 `appkey + ifaceName` 建立两层路由表：

```java
// 扫描包路径：com.sankuai.worldtree.debugservice.server.service
// 每个 AbstractDebugService 子类必须实现：
//   getAppkey()   → 表明自己服务于哪个下游 appkey
//   getIfaceName() → 表明自己对应哪个接口名

debugServiceMap = {
    "com.sankuai.wmdrecsys.rec.hub": {
        "recall"    → RecHubRecallService,
        "rankSpu"   → RecHubRankSpuService,
        "rankSpuV2" → RecHubRankSpuV2Service,
        "mix"       → RecHubMixService,
    },
    "com.sankuai.wmdrecsys.river": {
        "recall"    → RiverRecallService,
    },
    "com.sankuai.wmdrecsys.libra.rankserver": {
        "rank"      → LibraRankService,
    },
}
```

路由查找逻辑（`WorldtreeDebugService.run()`）：

```java
public Pair<Boolean, String> run(String appkey, String iface, AbstractDebugParams params) {
    // 特例：iface 为空且是 CommonParams → 走通用 Thrift 泛化调用
    if (StringUtils.isEmpty(iface) && params instanceof CommonParams) {
        return commonDebugService.process(params);
    }
    // 常规路由：appkey + iface 二级查找
    AbstractDebugService service = debugServiceMap.get(appkey).get(iface);
    return service.process(params);
}
```

### 3.2 核心执行模板（AbstractDebugService.process）

所有调试执行都遵循同一个**模板方法**，子类只需实现 `initRequest()` 和 `runIface()`：

```
process(params)
│
├─ ① initRequest(params)         [子类实现]
│      将前端参数构造为目标服务的 Thrift 请求对象（TBase 子类）
│
├─ ② beforeRequest(params)       [基类实现，最核心]
│      向 MTrace Context 注入图策略 Key 和调试等级
│      全链路模式下查 Lion 获取调用链，为各下游服务分别注入
│
├─ ③ runIface(req, params)       [子类实现]
│      发起 Thrift RPC 调用目标推荐服务
│      目标服务读取 MTrace Context → 执行指定图策略 DAG
│      SDK 在每个算子边界 hook → 采集输入输出 → 异步发 Mafka
│
├─ ④ getTraceId()                [基类实现]
│      Tracer.id() → 本次请求的全局唯一 traceId
│
└─ ⑤ afterResponse(req,res,params) [基类实现]
       构造 GraphDebugRequestWrapper（traceId/appKey/scene/requestParams）
       调用 requestService.saveOrUpdate() 先落库「壳」记录
       返回 Pair<true, traceId##scene>
```

### 3.3 通用泛化调用（CommonDebugService）

对于路由表中没有注册的服务，或前端直接指定 host/port 的场景，走 Thrift **泛化调用**路径：

```java
// 按 host#serviceName 缓存代理（Caffeine，1小时过期）
ThriftClientProxy proxy = thriftClientProxyCache.get(
    host + "#" + serviceName,
    () -> buildThriftClientProxy(appkey, host, serviceName, port)
);

// 通过 GenericService 反射调用任意 Thrift 方法
GenericService genericService = (GenericService) proxy.get();
String result = genericService.$invoke(methodName, paramTypes, paramValues);
```

---

## 四、图策略执行：SDK 侧的数据采集

这部分运行在**下游推荐服务内部**，由 `debugservice-sdk` 埋点提供。

### 4.1 算子信息采集（OperatorInformationCollector）

每个算子执行完毕后触发，并行序列化输入输出后异步发送：

```java
// debugservice-sdk/.../collect/support/OperatorInformationCollector.java

void collect(sceneId, graphKey, opName, inputs[], outputs[], params, exceptionInfo, runTime, status) {

    // 1. 构造 OperatorMessage
    OperatorMessage msg = new OperatorMessage();
    msg.setTraceId(Tracer.id());
    msg.setSpanId(Tracer.getSpanId());   // 记录调用层级，区分主图/子图
    msg.setAppkey(DebugConfigUtils.getCurrentAppkey());
    msg.setOpName(opName);
    msg.setRunTime(runTime);

    // 2. 并行序列化输入输出（CountDownLatch 等待）
    CountDownLatch latch = new CountDownLatch(isCollectUnitTest ? 4 : 2);

    DebugTaskThreadPool.submitTask(() -> {
        for (Object input : inputs) {
            inputsMapper.add(InOutputSerializeUtils.formatWithType(opName, input));
        }
        latch.countDown();
    });
    DebugTaskThreadPool.submitTask(() -> {
        for (Object output : outputs) {
            outputsMapper.add(InOutputSerializeUtils.formatWithType(opName, output));
        }
        latch.countDown();
    });

    // Level >= 3（单测模式）额外采集带完整类型信息的 Unit Test 格式数据
    if (debugLevel >= LEVEL_THREE || isForceCollectUnitTestData()) {
        // 再开 2 个线程，采集 inputsUnit / outputsUnit
    }

    latch.await();  // 等待序列化完成，再发送

    // 3. 异步发送 Mafka（不阻塞算子主流程）
    DebugTaskThreadPool.submitTask(() -> {
        MessageWrapper wrapper = MessageSerializer.serialize(msg);
        IDebugInformationSender sender = DebugInformatinSenderFactory.getSender(wrapper);
        sender.send(wrapper);   // → topic_worldtree_debug_info_v2
    });
}
```

### 4.2 图级信息采集（GraphInformationCollector）

图策略整体执行完毕时触发，记录执行时长、状态和异常：

```java
// debugservice-sdk/.../collect/support/GraphInformationCollector.java

void collect(sceneId, graphKey, graphVersion, graphType, runTime, status, exception) {
    GraphMessage msg = new GraphMessage();
    msg.setTraceId(Tracer.id());
    msg.setSpanId(Tracer.getSpanId());      // spanId 标识主图(0)还是子图(非0)
    msg.setGraphType(graphType);             // 0=主图, 1=子图
    msg.setGraphVersion(graphVersion);
    msg.setGraphExecuteTime(runTime);
    msg.setGraphStatus(status);
    msg.setGraphException(exception);

    MessageWrapper wrapper = MessageSerializer.serialize(msg);
    DebugInformatinSenderFactory.getSender(wrapper).send(wrapper);
}
```

### 4.3 消息格式

```
MessageWrapper {
    className : "com.sankuai...GraphDebugOperatorVO"   // 类型鉴别器，消费端据此反序列化
    pbSerialize: byte[]                                // Protostuff 序列化的 payload
}

序列化流程：
  GraphMessage / OperatorMessage
    └─ MessageSerializer.serialize()
        ├─ Protostuff 序列化 → byte[]
        └─ 封装进 MessageWrapper { className, pbSerialize }
            └─ JSON 序列化 → String → Mafka topic_worldtree_debug_info_v2
```

---

## 五、数据接收与存储

### 5.1 Mafka 消费（WorldtreeDebugInfoConsumer）

服务启动时（`@PostConstruct`）注册消费者，按消息中的 `className` 路由处理：

```java
// 消费配置
topic    = "topic_worldtree_debug_info"
group    = "group_worldtree_debugservice_info"
namespace = "waimai"
appkey   = "com.sankuai.worldtree.debugservice"

// 消费逻辑
consumer.recvMessageWithParallel(MessageWrapper.class, (message, ctx) -> {
    String className = wrapper.getClassName();
    if (className == GraphDebugOperatorVO.class.getName()) {
        // 算子数据：Protostuff 反序列化 → GraphDebugOperatorWrapper → tbl_graph_debug_operator
        operatorService.consume(
            new GraphDebugOperatorWrapper(
                ProtostuffUtil.deserialize(wrapper.getPbSerialize(), GraphDebugOperatorVO.class)
            )
        );
    } else if (className == GraphDebugRequestVO.class.getName()) {
        // 图请求数据：Protostuff 反序列化 → GraphDebugRequestWrapper → tbl_graph_debug_request
        requestService.consume(
            new GraphDebugRequestWrapper(
                ProtostuffUtil.deserialize(wrapper.getPbSerialize(), GraphDebugRequestVO.class)
            )
        );
    }
    return CONSUME_SUCCESS;  // 失败时返回 RECONSUME_LATER，最多重试 3 次
});
```

### 5.2 Request 的 SaveOrUpdate 合并策略

数据库以 `(traceId, appKey, scene)` 为业务唯一键，写入时按以下规则合并：

```
saveOrUpdate(wrapper)
│
├─ 查询已有记录（条件：traceId + appKey + scene）
│
├─ [不存在] → INSERT 新记录
│
└─ [已存在，可能同时有主图 + 多个子图记录]
    │
    ├─ [wrapper.requestParams 非空] ← 来源是前端触发的调试请求
    │   → 找 isChildGraph=0 的主图记录
    │   → UPDATE（覆盖请求参数，保留 memo）
    │
    └─ [wrapper.requestParams 为空] ← 来源是下游 SDK 上报
        → 按 graphKey 匹配已有记录
        → [匹配到] UPDATE（不覆盖 requestParams 和 memo）
        → [未匹配] INSERT 新记录（新的子图）
        → [主键冲突] 等待 500ms 后递归重试（应对主从延迟）
```

### 5.3 大字段压缩存储

请求/响应/调试信息等大字段统一使用 **Gzip 压缩** 存入 BLOB 列，透明解压：

```java
// 写入时压缩（insertOrUpdate 内部）
byte[] compressed = GzipUtil.compress2Bytes(po.getRequestString());
po.setRequestString("");                   // 清空文本字段
po.setRequestStringCompressed(compressed); // 写入 BLOB 字段

// 读取时解压（selectDecompress 回调，MyBatis 查询后触发）
if (isEmpty(po.getRequestString()) && po.getRequestStringCompressed() != null) {
    po.setRequestString(GzipUtil.decompressBytes(po.getRequestStringCompressed()));
    po.setRequestStringCompressed(null);
}
```

受此压缩策略保护的字段：

| 字段                  | 对应 BLOB 列                        |
|-----------------------|-------------------------------------|
| `requestString`       | `requestStringCompressed`           |
| `responseString`      | `responseStringCompressed`          |
| `requestBusiDebug`    | `requestBusiDebugCompressed`        |
| `inputList`（算子表） | `inputListCompressed`               |
| `outputList`（算子表）| `outputListCompressed`              |

---

## 六、数据查询与结果渲染

### 6.1 查询维度

| 接口                       | 查询条件                                      | 用途说明                         |
|----------------------------|-----------------------------------------------|----------------------------------|
| `page()`                   | traceId/graphKey/mis/scene + requestParams 非空| 列表页，只展示用户主动触发的请求 |
| `getAllRequest()`           | traceId/graphKey + isChildGraph=0             | 展开页，只看主图，附加子图信息   |
| `queryServerInfoByTrace()` | traceId，按 span 升序                         | 查调用链上所有节点信息           |
| `findBusiDebug()`          | traceId + appKey → 主 request 的 busiDebug   | 获取请求级别的业务调试摘要       |

`getAllRequest()` 在返回主图记录后，还会为每条记录查询 `graphKeyInfos`（当前 traceId 下的所有图策略版本），供前端展示完整的调用链路图版本情况。

### 6.2 结果渲染流程（RenderResponseService）

```
render(traceId, appKey)
│
├─① renderDebugInfo()
│   ├─ findMainRequest(traceId, appkey)
│   │   └─ 优先返回有 responseString 的记录
│   │      其次返回有 requestBusiDebug 且 busiRequestString 非空的记录
│   └─ 解析 requestBusiDebug（JSON: Map<算子名, Map<指标名, Object>>）
│      → 过滤掉 ItemDebug（商品粒度数据过大，不在摘要中展示）
│      → 返回给前端的请求级调试摘要
│
└─② renderItems()
    ├─ 根据 (appkey, scene) 从路由表找到对应的 AbstractDebugService
    └─ 调用 debugService.renderResponseItems(vo)
       → 子类自定义商品列表的反序列化和格式化逻辑
       → 结果写入 vo.responseString（JSON 字符串）
```

---

## 七、调试等级体系

| 等级    | 触发方式                           | 数据采集范围                             | 典型场景             |
|---------|------------------------------------|------------------------------------------|----------------------|
| Level 0 | 正常线上请求（无 MTrace Context）  | 不上报任何数据                           | 生产流量，零开销     |
| Level 1 | 用户 ID 命中 Lion 白名单           | 上报图级摘要（无算子输入输出）           | 灰度常态观察         |
| Level 2 | 调试平台发起（前端触发）           | 全量图信息 + 算子输入输出               | 正常调试分析         |
| Level 3 | 调试平台发起 + 单测模式            | Level 2 内容 + 带完整类型的 Unit Test 数据 | 自动生成单元测试用例 |

Level 0 通过 `admittanceProcess()` 在 SDK 入口处快速返回，对推荐服务性能**零影响**。

---

## 八、数据模型

### 8.1 tbl_graph_debug_request（图调试请求主表）

```
字段分组：
├── 定位字段：traceId, appKey, scene, graphKey, version
├── 服务信息：hostName, hostIp, servicePort, serviceName, methodName
├── 主/子图：isChildGraph (0=主图, 1=子图)
├── 请求存储：requestString / requestStringCompressed(BLOB) / requestBinary(BLOB)
├── 响应存储：responseString / responseStringCompressed(BLOB)
├── 业务调试：requestBusiDebug / requestBusiDebugCompressed(BLOB)
├── 前端参数：requestParams（前端发起时的完整入参 JSON，用于标识主图）
├── 用户身份：userId, mis, uuid
├── 状态跟踪：status, exceptionInfo, memo
└── 链路信息：span（MTrace spanId，标识调用层级）, createTime
```

### 8.2 tbl_graph_debug_operator（算子调试记录表）

```
字段分组：
├── 定位字段：traceId, opName, appKey, graphKey, graphVersion, scene
├── 主/子图：isChildGraph, childGraphKey
├── 执行数据：inputList / inputListCompressed, outputList / outputListCompressed
├── 调试信息：busiDebug / busiDebugCompressed, param（算子参数）
├── 状态信息：status, exceptionInfo, runTime（毫秒）
└── 链路信息：span, createTime
```

---

## 九、完整时序图

```
前端          DebugController   AbstractDebugService   推荐服务(RecHub)   SDK            Mafka         Consumer         DB
 │                 │                   │                    │               │               │                │              │
 │─POST /debug/rechub→│                │                    │               │               │                │              │
 │                 │──run(appkey,iface)─→│                  │               │               │                │              │
 │                 │                   │─① initRequest()   │               │               │                │              │
 │                 │                   │─② beforeRequest() │               │               │                │              │
 │                 │                   │  Tracer.putContext("worldtree_xxx","rec_hub_v2")   │                │              │
 │                 │                   │  Tracer.putContext("worldtreeDebugLevel","2")      │                │              │
 │                 │                   │─③ runIface()─────→│               │               │                │              │
 │                 │                   │                    │─读MTrace Context→            │               │              │
 │                 │                   │                    │  执行指定图策略DAG            │               │              │
 │                 │                   │                    │  算子1执行完毕 ─collect()──→│               │              │
 │                 │                   │                    │               │─序列化输入输出 │               │              │
 │                 │                   │                    │               │─send()───────→│               │              │
 │                 │                   │                    │  算子2执行完毕 ─collect()──→│─send()───────→│              │
 │                 │                   │                    │  图执行完毕    ─collect()──→│─send()───────→│              │
 │                 │                   │←───────────────────RPC 响应       │               │                │              │
 │                 │                   │─④ getTraceId()    │               │               │                │              │
 │                 │                   │─⑤ afterResponse() │               │               │                │              │
 │                 │                   │   saveOrUpdate(壳记录)──────────────────────────────────────────────────────────→│ INSERT
 │←─返回 traceId───│                   │                    │               │               │                │              │
 │                 │                   │                    │               │               │──消费消息──────→│              │
 │                 │                   │                    │               │               │  Protostuff反序列化           │              │
 │                 │                   │                    │               │               │────────────────→│saveOrUpdate()│
 │                 │                   │                    │               │               │                │ Gzip压缩写入 │
 │                 │                   │                    │               │               │                │─────────────→│ UPDATE/INSERT
 │─GET /debug/request/all──────────────────────────────────────────────────────────────────────────────────────────────────────────────→│ SELECT
 │←─GraphDebugRequestWrapper(含graphKeyInfos)────────────────────────────────────────────────────────────────────────────────────────────│
 │  renderResponseItems()（自定义商品列表渲染）                                                                                          │
 │←─调试结果（responseString + requestBusiDebug）                                                                                        │
```

---

## 十、关键设计决策总结

| 设计点            | 实现方式                                                         | 核心价值                                           |
|-------------------|------------------------------------------------------------------|----------------------------------------------------|
| **图策略注入**    | MTrace Context 透传，Key=`worldtree_{appkey}`                    | 零侵入下游服务，自动随 RPC 调用链传递              |
| **多服务路由**    | Lion 配置 scene→调用链拓扑，启动时扫描 Bean 建二级路由表         | 支持 RecHub+River+Libra 全链路一键调试             |
| **数据采集解耦**  | SDK 异步发 Mafka，Consumer 独立消费写库                          | 不阻塞推荐服务主流程，不影响线上性能               |
| **调试等级**      | 4 级精细控制（Level 0-3），Level 0 在 SDK 入口快速返回           | 生产安全，白名单用户可常态轻量采集                 |
| **主/子图合并**   | `(traceId,appKey,scene)` 业务键 + `graphKey` + `isChildGraph`   | 一次调试可关联主图+多个子图的完整执行情况          |
| **大字段存储**    | Gzip 压缩写 BLOB，查询时透明解压，回调函数模式统一处理           | 应对推荐系统大请求体（商品列表、特征向量等）       |
| **并发序列化**    | CountDownLatch + 独立线程池并行序列化输入/输出                   | 加速算子数据采集，最小化对算子执行时间的影响       |
| **重试与幂等**    | DuplicateKeyException 捕获 + sleep(500ms) 递归重试               | 应对 MySQL 主从同步延迟导致的并发写入冲突          |

---

## 十一、相关源码文件索引

| 功能模块             | 关键文件路径                                                                                   |
|----------------------|-----------------------------------------------------------------------------------------------|
| **调试触发入口**     | `debugservice-server/.../controller/DebugController.java`                                     |
| **v2 入口**          | `debugservice-server/.../v2/controller/DebugCommonRequestController.java`                     |
| **服务路由**         | `debugservice-server/.../service/impl/WorldtreeDebugService.java`                             |
| **执行模板**         | `debugservice-server/.../service/impl/AbstractDebugService.java`                              |
| **通用泛化调用**     | `debugservice-server/.../service/CommonDebugService.java`                                     |
| **请求落库**         | `debugservice-server/.../service/impl/GraphDebugRequestService.java`                          |
| **算子落库**         | `debugservice-server/.../service/impl/GraphDebugOperatorService.java`                         |
| **结果渲染**         | `debugservice-server/.../service/impl/RenderResponseService.java`                             |
| **Mafka 消费**       | `debugservice-server/.../mafka/WorldtreeDebugInfoConsumer.java`                               |
| **SDK 全局上下文**   | `debugservice-sdk/.../DebugGlobalContext.java`                                                |
| **算子数据采集**     | `debugservice-sdk/.../collect/support/OperatorInformationCollector.java`                      |
| **图数据采集**       | `debugservice-sdk/.../collect/support/GraphInformationCollector.java`                         |
| **Mafka 生产者**     | `debugservice-sdk/.../report/support/MafkaDebugInformationSender.java`                       |
| **Request 数据模型** | `debugservice-dao/.../domain/GraphDebugRequestPO.java`                                        |
| **Operator 数据模型**| `debugservice-dao/.../domain/GraphDebugOperatorPO.java`                                       |
| **调试常量**         | `debugservice-api/.../constant/DebugConstants.java`                                           |
| **调试等级枚举**     | `debugservice-api/.../enums/DebugLevelEnum.java`                                              |
