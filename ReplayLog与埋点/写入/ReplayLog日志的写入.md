# RiverServiceImpl
```java
@Service("riverService")
public class RiverServiceImpl implements RiverService.Iface {
    private static final Logger replayLogger = LoggerFactory.getLogger("logger_wmrec_river_replaylog");
    
    @Override
    public Response recall(Request request) throws TException {
        ContextBase contextBase = initContextBase(request);// 初始化 ContextBase，每次请求 new 一个 ContextBase
        // ... 业务逻辑、图策略处理逻辑
        recordReplayLog(contextBase);// 记录日志
        
        Response response = contextBase.getRiverResponse();

        return response;
    }
    
    @Override
    private ContextBase initContextBase(Request request) {
        ContextBase contextBase = new ContextBase();
        contextBase.setRequest(request);
        
        Response response = new Response();
        contextBase.setResponse(response);
        
        contextBase.setExtraParams(MapUtils.emptyIfNull(request.getExtraParams()));
        
        return contextBase;
    }
    
    private void recordReplayLog(ContextBase contextBase) {
        FutureTaskThreadPool.getExecutorService().submit(() -> {
            RiverReplayLog replayLog = contextBase.getReplayLog().initBaseInfo(contextBase);//  ① 初始化
            String replayLogStr = replayLog.convert2logString();// ② 再序列化
            if (replayLogStr.getBytes().length >= MAX_LOG_BYTE_LENGTH) {
                Cat.logEvent("replay_log_out_1m", contextBase.getGraphScene());
            }
            replayLogger.info(replayLogStr);// ③ 写入日志
        });
    }
}
```
# ContextBase.java

```java
@Setter
@Getter
@Slf4j
public class ContextBase implements ExecutionEnvironment {

    @Getter
    private RiverReplayLog replayLog;

    public ContextBase() {// 构造函数
    // ...其他初始化
    replayLog = new RiverReplayLog();   // 随 ContextBase 一起诞生
    }
}
```
# RiverReplayLog.java
```java
@Data
public class RiverReplayLog {
    private long userId;
    private String uuid;
    private String bizTraceId;
    // ...其他字段
    private String request;
    private String response;
    
    // Map 字段用 ConcurrentHashMap 保证并发安全
    private Map<String, List<Long>> originRecalls = new ConcurrentHashMap<>();// 日志埋点写入-原始召回结果
    private Map<String, Object> extendData = new ConcurrentHashMap<>();// 日志埋点写入-其他数据
    private Map<String, Object> message = new ConcurrentHashMap<>();// 日志埋点写入-消息（大数据量信息）
    
    public RiverReplayLog initBaseInfo(ContextBase contextBase) {
        this.setUserId(contextBase.getUserId());
        this.setUuid(contextBase.getUuid());
        this.setBizTraceId(contextBase.getBizTraceId());
        
        this.setRequest(ThriftJsonSerializerUtil.getJsonString(contextBase.getRiverRequest()));
        this.setResponse("");// 初始化占位，避免npe
        
        return this;
    }
}
```
## 日志写入机制
两层机制：SLF4J Logger + XMDLogFormat
replayLogger 是一个普通 SLF4J Logger，logger name 对应 Logback 配置中的一个 Appender，负责路由到对应的日志文件/采集管道。
LoggerFactory.getLogger引入的包:
```xml
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>1.7.5</version>
    </dependency>
```

## 实例的完整生命周期
recall(request) 调用
│
├─ new ContextBase()
│    └─ new RiverReplayLog()   ← 实例诞生，Map 字段空
│
├─ graphStrategy.start(contextBase)   ← 图执行（同步）
│    └─ 各算子并发写入 replayLog 的 Map 字段
│         originRecalls.put(...)
│         extendData.put(...)
│
├─ recordReplayLog(contextBase)       ← 图执行完毕后触发
│    └─ FutureTaskThreadPool.submit(lambda)   ← 异步提交，立即返回
│         lambda 持有 contextBase 引用，阻止 GC
│
├─ return response                    ← 主线程返回给调用方
│
└─ [异步线程] lambda 执行
├─ replayLog.initBaseInfo(contextBase)   ← 补充基础字段
├─ replayLogger.info(replayLogStr)        ← 写入日志
└─ lambda 结束，contextBase 引用释放
└─ ContextBase & RiverReplayLog 等待 GC 回收  ← 实例消亡

关键点：主线程 return response 时，实例并没有立刻消亡。异步 lambda 持有 contextBase 引用，直到日志写完，JVM 才能回收。这意味着极端情况下线程池积压时，大量请求的 RiverReplayLog 会同时驻留内存。

## 一些问题
### 一次请求的数据是否全部写入同一个 RiverReplayLog 实例？
是的，整个请求生命周期共享同一个实例。 
原因在于生命周期与 ContextBase 完全绑定, RiverReplayLog在ContextBase的构造函数中初始化,执行图策略以后将所有需要落盘的信息补充到replayLog实例中，调用异步任务执行落盘。
ContextBase 实现了 ExecutionEnvironment 接口【worldtree架构实现，能够让上下文在算子间共享】，被作为执行环境传入图的所有算子，所以算子通过 contextBase.getReplayLog() 拿到的永远是同一个实例，然后通过ReplayLogUtil.writeReplayLog()写入replayLog中对应的map字段中。
**这样设计的原因是聚合：一次推荐请求跨越几十个算子，日志需要汇总成一条完整的链路记录，而不是每个算子各打各的。**

### 极端情况下线程池积压时，大量请求的 RiverReplayLog 会同时驻留内存?
**主线程 return response 时，实例并没有立刻消亡。异步 lambda 持有 contextBase 引用，直到日志写完，JVM 才能回收。**
JVM 的 GC 只回收没有任何引用指向的对象。ContextBase 是方法内的局部变量，正常情况下 recall() 方法返回后就会失去引用、等待 GC。

但这里有异步 lambda：
```java
private void recordReplayLog(ContextBase contextBase) {
    FutureTaskThreadPool.getExecutorService().submit(() -> {
        // 这个 lambda 捕获了外部变量 contextBase
        RiverReplayLog replayLog = contextBase.getReplayLog();  // ← 引用链
        replayLogger.info(replayLog.convert2logString());
    });
    // submit 立刻返回，lambda 进入线程池队列
}
```
```java
// recall() 方法继续执行：
public Response recall(Request request) throws TException {
    ContextBase contextBase = initContextBase(request);
    recordReplayLog(contextBase);
    Response response = contextBase.getRiverResponse();
    return response;   // ← recall() 结束，但 contextBase 没有被 GC！
}
```
```markdown
线程池队列（ArrayBlockingQueue）
└─ 待执行的 Runnable（lambda 闭包）
└─ 捕获的变量 contextBase   ← GC Root 可达
└─ replayLog
└─ extendData (ConcurrentHashMap)
└─ originRecalls (ConcurrentHashMap)
└─ ... (所有 Map 数据)

只要 lambda 没有被线程池执行完，这条引用链就不断，JVM 就不能回收,极端情况下的内存压力.
```
```markdown
看 FutureTaskThreadPool 的配置：
new ThreadPoolExecutor(
    50,                          // corePoolSize：常驻线程数
    150,                         // maximumPoolSize：最大线程数
    60L, TimeUnit.SECONDS,
    new ArrayBlockingQueue<>(2000)  // 队列容量：最多积压 2000 个任务
)

正常情况：线程池处理速度 > 提交速度，lambda 很快执行完，contextBase 立刻可被 GC。

积压情况（流量突增 / 日志 IO 慢 / 线程池打满）：
请求1 的 lambda ──→ 进队列（contextBase1 存活）
请求2 的 lambda ──→ 进队列（contextBase2 存活）
...
请求2000 的 lambda ─→ 进队列（contextBase2000 存活）
请求2001 ──→ 队列满，触发 RejectedExecutionException（日志丢失）
这 2000 个 contextBase 实例及其 RiverReplayLog（每个含若干 ConcurrentHashMap）全部驻留在堆内存里，直到线程池消化完队列才陆续释放。
若每条 ReplayLog 有几十KB 数据，2000 条就是几十到几百 MB 的额外内存占用。
```



