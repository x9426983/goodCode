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

## 一次请求的数据是否全部写入同一个 RiverReplayLog 实例？
是的，整个请求生命周期共享同一个实例。 原因在于生命周期与 ContextBase 完全绑定, RiverReplayLog在ContextBase的构造函数中初始化
ContextBase 实现了 ExecutionEnvironment 接口，被作为执行环境传入图的所有算子，所以算子通过 contextBase.getReplayLog() 拿到的永远是同一个实例。【worldtree架构实现，能够让上下文在算子间共享】
这样设计的原因是聚合：一次推荐请求跨越几十个算子，日志需要汇总成一条完整的链路记录，而不是每个算子各打各的。
