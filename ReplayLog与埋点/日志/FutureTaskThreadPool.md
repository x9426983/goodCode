# 是什么
mt自己封装的一个静态全局异步线程池，专门用来执行"不想阻塞主流程"的任务
## 作用
```plantuml
① 线程复用
不用每次 new Thread()，线程创建销毁代价大
池子里的线程跑完一个任务，接着跑下一个

② 流量控制
最多 150 个线程同时跑
最多 2000 个任务排队等待
超出就拒绝，保护服务不被压垮

③ Trace 透传（通过 ExecutorServiceTraceWrapper）
把主线程的 traceId 带进异步任务
日志链路不断
```
## 优点
```plantuml
和普通 new Thread() 相比
new Thread(() -> doTask()).start()     FutureTaskThreadPool.submit(task)
每次都新建线程 ← 慢                    复用已有线程 ← 快
没有数量限制 ← 可能撑爆内存            最多 150 线程 ← 安全
跑完就销毁 ← 浪费                      跑完回池子等下一个任务
没有返回值                             返回 Future，可以拿结果
traceId 断链                           TraceWrapper 自动透传
```
## 使用
```java
private void recordReplayLog(ContextBase contextBase) {
    FutureTaskThreadPool.getExecutorService().submit(() -> {
        RiverReplayLog replayLog = contextBase.getReplayLog().initBaseInfo(contextBase);
        String replayLogStr = replayLog.convert2logString();
        replayLogger.info(replayLogStr);
    });
}
```
# FutureTaskThreadPool与实现
```java
public class FutureTaskThreadPool {
    // 线程池的参数
    private static final int CORETHREADS = 50;
    private static final int MAXNTHREADS = 150;

    private static final ExecutorServiceTraceWrapper threadPool =
            new ExecutorServiceTraceWrapper(
                    new ThreadPoolExecutor(CORETHREADS,
                            MAXNTHREADS, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000),
                            new ThreadFactoryBuilder().setNameFormat("FutureTaskThreadPool-thread-%d").build()));


    public static ExecutorService getExecutorService() {
        return threadPool;
    }
    public static <T> Future<T> submit(Callable<T> task) {
        return threadPool.submit(task);
    }

    public static <T> Future<?> submit(Runnable task) {
        return threadPool.submit(task);
    }
}
```
## FutureTaskThreadPool的底层实现栈
业务代码
│ submit(Callable/Runnable)
▼
ExecutorServiceTraceWrapper          ← 装饰器层（MTTrace 透传）
│ delegate
▼
ThreadPoolExecutor                   ← JDK 核心实现
│
├── workers: HashSet<Worker>      ← 线程集合
├── workQueue: ArrayBlockingQueue ← 任务缓冲区
└── Worker extends AQS            ← 每个线程是一把锁
│
▼
Thread (内核线程 1:1)         ← OS 调度

## ThreadPoolExecutor 内部状态机

                  shutdown()
RUNNING ──────────────────────► SHUTDOWN ──┐
│                                        │ 队列空 & 线程空
│ shutdownNow()                          ▼
└──────────────────────────► STOP ──► TIDYING ──► TERMINATED
▲
所有Worker终止 ─┘

状态 + 线程数 打包在同一个 AtomicInteger ctl 里（高3位=状态，低29位=线程数）：

// JDK 源码
private final AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));
// RUNNING  = 111_00000...（高3位）
// 线程数   = 低29位
// 用一个 CAS 操作同时检查状态和线程数，避免两把锁

## 任务提交的完整路径（源码级）

submit(task)
│
▼
AbstractExecutorService.submit()
│  包装成 FutureTask
▼
ThreadPoolExecutor.execute(FutureTask)
│
├─[workerCount < corePoolSize]──► addWorker(task, true)
│                                  新建核心线程，直接执行
│
├─[workQueue.offer(task)]───────► 入队成功
│   │
│   └─ 双重检查（DCL）：
│      若此时线程池已 SHUTDOWN → remove(task) + reject
│      若此时 workerCount=0 → addWorker(null, false) 补充线程
│
└─[队列满，addWorker(task,false)]
│
├─ 成功 → 新建非核心线程执行
└─ 失败（超maxPoolSize）→ reject(task)
↓
AbortPolicy.rejectedExecution()
抛 RejectedExecutionException

## Worker 线程的生命周期

// Worker 的核心循环（runWorker方法）
while (task != null || (task = getTask()) != null) {
// getTask() 从队列取任务，阻塞等待
try {
task.run();   // 执行 FutureTask.run()
} finally {
task = null;
}
}
// 循环退出 → 线程销毁

getTask() 的阻塞行为决定了线程的存活：

核心线程                    非核心线程
│                           │
workQueue.take()          workQueue.poll(keepAlive, unit)
│                           │
永久阻塞等待               等待 60 秒后返回 null
│                           │
常驻内存                 超时后线程自销毁


## ArrayBlockingQueue vs 其他队列的本质区别

                    ┌─────────────────────────────────────────┐
                    │           队列类型对比                    │
┌───────────────────┼──────────────┬──────────────┬───────────┤
│ 队列类型           │ 有界/无界    │ 锁机制        │ 适用场景   │
├───────────────────┼──────────────┼──────────────┼───────────┤
│ArrayBlockingQueue │ 有界(2000)  │ 单ReentrantLock│ 本项目   │
│LinkedBlockingQueue│无界(Integer │ 双锁(头/尾分离)│ 高吞吐   │
│                   │ .MAX_VALUE)   │               │           │
│SynchronousQueue   │ 容量=0       │ CAS           │ 直接交付  │
│PriorityBlockingQ  │ 无界         │ ReentrantLock │ 优先级调度│
└───────────────────┴──────────────┴──────────────┴───────────┘

选 ArrayBlockingQueue 的含义：
- 背压机制：队列满时拒绝新任务，而非无限堆积 OOM
- 内存可预测：数组预分配，无链表节点 GC 压力
- 单锁代价：入队出队共用一把锁，高并发下竞争比 LinkedBlockingQueue 激烈

## FutureTask 的内部状态机

submit(Callable) 返回的 Future 对象本质是 FutureTask：

    NEW ──────── run() ──────► COMPLETING ──► NORMAL（有结果）
     │                                    └─► EXCEPTIONAL（有异常）
     │
     ├── cancel(false) ──────────────────► CANCELLED
     └── cancel(true)  ──────────────────► INTERRUPTING ──► INTERRUPTED

future.get() 的阻塞原理（非自旋，基于 LockSupport）：

// FutureTask 内部
private Object outcome;   // 存储结果或异常
private volatile int state;  // 状态（volatile 保证可见性）

// get() 调用时：
if (state == NORMAL)  return outcome;   // 已完成，直接返回
else LockSupport.park(this);            // 未完成，挂起当前线程（不消耗CPU）

// 任务完成时：
outcome = result;
state = NORMAL;           // volatile write，happens-before 保证
LockSupport.unpark(等待线程);  // 唤醒

## ExecutorServiceTraceWrapper 的作用原理

正常 submit：

主线程(traceId=ABC) ──► submit(task) ──► 线程池线程执行 task
↓
traceId = null  ← 断链！

加 TraceWrapper 后：

主线程(traceId=ABC) ──► submit(task)
│
┌─────────▼──────────────────┐
│  TraceWrapper 拦截           │
│  capturedTrace = 当前traceId │
│  包装成：                    │
│  () -> {                    │
│    MTTrace.set(capturedTrace)│ ← 恢复 traceId
│    task.run()               │
│    MTTrace.clear()          │
│  }                          │
└─────────────────────────────┘
│
线程池线程执行包装后的任务
traceId=ABC ✅ 链路完整

本质是 ThreadLocal 的跨线程传递，通过装饰器模式在任务执行前恢复上下文。

## CompletableFuture 与线程池的组合理论

CompletableFuture 的执行模型：

supplyAsync(supplier, executor)
│
▼
Stage1: 提交到 executor（FutureTaskThreadPool）

      .thenApply(fn)    ← 若 Stage1 未完成，注册回调
                           若 Stage1 已完成，在完成该Stage1的线程上执行

      .thenApplyAsync(fn, executor)  ← 强制提交到指定 executor

      .thenAccept(consumer)  ← 同 thenApply，无返回值

allOf().join() 的底层：

CompletableFuture.allOf(f1, f2, f3)
│
创建一个新 Future，监听所有子 Future
每个子 Future 完成时 → CAS 递减计数器
计数器归零 → complete(null) → 唤醒 join() 的等待线程

join() vs get():
get()  → 抛受检异常（InterruptedException/ExecutionException）
join() → 抛非受检异常（CompletionException）
本质相同，均基于 LockSupport.park

## 理论总结

FutureTaskThreadPool 涉及的核心理论层次：

┌─ 并发理论 ───────────────────────────────────────────┐
│  • happens-before（volatile + unlock→lock）          │
│  • CAS（ctl 原子状态）                                │
│  • AQS（Worker 继承 AbstractQueuedSynchronizer）      │
└──────────────────────────────────────────────────────┘

┌─ 线程调度理论 ────────────────────────────────────────┐
│  • LockSupport.park/unpark（非自旋挂起，零CPU等待）    │
│  • OS 1:1 线程模型（Java 线程 = 内核线程）             │
│  • 上下文切换代价（max=150 的设计依据）                │
└──────────────────────────────────────────────────────┘

┌─ 设计模式 ────────────────────────────────────────────┐
│  • 装饰器模式（TraceWrapper）                         │
│  • 生产者-消费者（任务队列）                           │
│  • Future 模式（异步结果凭证）                         │
│  • 责任链（CompletableFuture 流水线）                  │
└──────────────────────────────────────────────────────┘

┌─ 背压与流量控制理论 ──────────────────────────────────┐
│  • 有界队列 = 背压信号（满则拒绝，不 OOM）             │
│  • 超时控制 = 资源隔离（防止慢调用拖垮线程池）          │
│  • 核心/最大线程分离 = 弹性伸缩（常态/洪峰双适配）     │
└──────────────────────────────────────────────────────┘
