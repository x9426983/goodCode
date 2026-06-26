**ThreadPoolExecutor** 是 Java 并发编程中**线程池的核心实现类**，位于 `java.util.concurrent` 包中。它提供了一个强大且灵活的方式来管理和复用线程，以执行大量异步任务。

---

## **核心组成**

ThreadPoolExecutor 通过几个关键参数来控制线程池的行为：

### **1. 核心参数**
- **corePoolSize**：核心线程数，线程池维护的最小线程数量（即使空闲也不会被回收，除非设置 `allowCoreThreadTimeOut`）。
- **maximumPoolSize**：最大线程数，线程池允许创建的最大线程数量。
- **keepAliveTime**：空闲线程存活时间（超出核心线程数的空闲线程，在指定时间后会被回收）。
- **unit**：存活时间单位（如 `TimeUnit.SECONDS`）。
- **workQueue**：任务队列，用于存放待执行的任务（如 `LinkedBlockingQueue`、`ArrayBlockingQueue`）。
- **threadFactory**：线程工厂，用于创建新线程（可自定义线程名、优先级等）。
- **handler**：拒绝策略，当任务太多（队列满且线程数达到最大值）时如何处理新任务。

### **2. 拒绝策略（RejectedExecutionHandler）**
- `AbortPolicy`（默认）：直接抛出 `RejectedExecutionException`。
- `CallerRunsPolicy`：用调用者所在线程来执行任务。
- `DiscardPolicy`：直接丢弃任务，不抛异常。
- `DiscardOldestPolicy`：丢弃队列中最旧的任务，然后尝试重新提交。

---

## **工作原理**
1. 提交任务时，优先创建核心线程（直到达到 `corePoolSize`）。
2. 如果核心线程都在忙，新任务进入**工作队列**等待。
3. 如果队列已满，则创建新线程（直到达到 `maximumPoolSize`）。
4. 如果线程数已达最大值且队列已满，则触发**拒绝策略**。

---

## 使用示例
### execute
```java
import java.util.concurrent.*;
public class ThreadPoolExample { 
    public static void main(String[] args) {
        // 创建线程池
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,// 核心线程数
                5,// 最大线程数
                60,// 空闲线程存活时间
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10), // 任务队列容量
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略
        );
        // 提交任务
        for (int i = 0; i < 15; i++) {
            final int taskId = i;
            executor.execute(() -> {
                System.out.println("执行任务: " + taskId + ", 线程: " + Thread.currentThread().getName());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        // 关闭线程池
        executor.shutdown();
    }
}
```
### submit
ThreadPoolExecutor的submit() 方法是比 execute() 更强大的任务提交方式，返回一个 Future 对象，允许跟踪任务的执行状态和获取结果。
```java

```

---

## **常用创建方式（通过 Executors 工厂类）**
虽然直接使用 `ThreadPoolExecutor` 更灵活，但 Java 提供了便捷的工厂方法：
- `Executors.newFixedThreadPool(n)`：固定大小线程池。
- `Executors.newCachedThreadPool()`：可缓存线程池（无线程上限）。
- `Executors.newSingleThreadExecutor()`：单线程池。
- `Executors.newScheduledThreadPool(n)`：定时任务线程池。

> **注意**：`Executors` 创建的线程池可能隐藏资源耗尽风险（如 `newFixedThreadPool` 使用无界队列），生产环境建议根据业务需求直接配置 `ThreadPoolExecutor`。

---

## **主要优点**
1. **降低资源消耗**：复用已创建的线程，避免频繁创建/销毁开销。
2. **提高响应速度**：任务到达时无需等待线程创建。
3. **管理线程生命周期**：统一分配、监控和调优。
4. **控制并发度**：避免无限制创建线程导致系统崩溃。

---

## **注意事项**
- 合理配置参数（根据任务类型：CPU 密集型 vs IO 密集型）。
- 一定要调用 `shutdown()` 或 `shutdownNow()` 优雅关闭线程池。
- 避免使用无界队列（可能导致内存溢出）。
- 监控线程池状态（如通过 `getActiveCount()`、`getQueue().size()`）。

---

## **总结**
`ThreadPoolExecutor` 是 Java 线程池的“发动机”，理解其机制和参数配置对编写高性能并发程序至关重要。实际开发中应根据任务特性（耗时、优先级、依赖性）灵活定制，而非盲目使用默认配置。