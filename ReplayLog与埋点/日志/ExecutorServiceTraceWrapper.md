# ExecutorServiceTraceWrapper
## 在本项目中的具体实例
```markdown
// FutureTaskThreadPool.java
new ExecutorServiceTraceWrapper(      // ← 外层装饰器
    new ThreadPoolExecutor(...)        // ← 内层真实线程池
)
```
ExecutorServiceTraceWrapper 和 ThreadPoolExecutor 实现同一个接口，
在 submit 时用 forFork() 在主线程拍一份上下文快照，用 doFork() 在工作线程还原，
本质是手动把 ThreadLocal 变量跨线程搬运，让异步任务的日志和监控能自动归属到触发它的那次用户请求上。
```plantuml
两者的分工边界总结：
// Wrapper 把包装好的任务交给 ThreadPoolExecutor
return m_executorService.submit(CatTraceCallable.get(task, p));
//                              ↑ 到这里 Wrapper 的活就做完了
//     ↑ 这里往后全是 ThreadPoolExecutor 的事
```

## 源码级执行流程
```textmate
以 submit(Callable) 为例，完整调用链：

// ① 业务代码调用
FutureTaskThreadPool.submit(() -> doHttpCall())

      ↓

// ② CatExecutorServiceTraceWrapper.submit()（装饰器拦截）
public <E> Future<E> submit(Callable<E> task) {
    Transaction t = Cat.newTransaction("System", "ExecutorService");
    ForkableTransaction p = t.forFork();   // ← 在主线程捕获当前 Trace 上下文快照
    try {
        return m_executorService.submit(
            CatTraceCallable.get(task, p)  // ← 将任务包装，携带上下文快照
        );
    } finally {
        t.setSuccessStatus();
        t.complete();
    }
}

      ↓

// ③ CatTraceCallable 被放入线程池队列
// （此时主线程已经返回，traceId 在 ThreadLocal 里只属于主线程）

      ↓

// ④ 线程池线程从队列取出任务，执行 call()
public V call() throws Exception {
    try (ForkedTransaction p = m_transaction.doFork()) {
        // doFork() 将第②步捕获的 traceId 恢复到当前线程的 ThreadLocal
        return m_callable.call();   // ← 真正的业务逻辑，此时 traceId 已存在
    }
    // try-with-resources 自动 close → 清理当前线程的 ThreadLocal
}
```
## 交互
```
主线程
  │
  │  submit(原始task)
  ▼
ExecutorServiceTraceWrapper
  │  我的活：给任务贴上 traceId 标签
  │  task → CatTraceCallable(task, snapshot)
  │
  │  submit(贴了标签的task)
  ▼
ThreadPoolExecutor
  │  我的活：找线程来跑这个任务
  │  不管任务内容是什么，照样调度
  │
  │  thread.run(贴了标签的task)
  ▼
线程池线程
     │  先恢复 traceId（标签的逻辑）
     │  再执行业务代码（原始task）
     │  最后清理 traceId

```
## 作用
ThreadLocal 的跨线程陷阱
```textmate
没有 TraceWrapper 时：

主线程                      线程池线程
  │                              │
  │ ThreadLocal                  │ ThreadLocal
  │ traceId = "ABC-123"          │ traceId = null  ← 新线程，没有继承
  │                              │
  │─── submit(task) ────────────►│
  │                              │ task.run()
  │                              │   Tracer.id() → null ← 链路断了！
  │                              │   下游日志无法关联到同一次请求

有 TraceWrapper 后：

主线程                      线程池线程
  │                              │
  │ traceId = "ABC-123"          │
  │                              │
  │ forFork() 快照               │
  │ snapshot = {traceId:"ABC-123"}│
  │─── submit(wrapped task) ────►│
  │                              │ doFork(snapshot)
  │                              │ ThreadLocal.set("ABC-123")  ← 恢复！
  │                              │ task.run()
  │                              │   Tracer.id() → "ABC-123" ✅
  │                              │ finally: ThreadLocal.remove() ← 清理，防泄漏
```


