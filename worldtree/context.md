框架不知道 ContextBase 是什么，只知道它是 ExecutionEnvironment。
River 利用接口把请求上下文"注入"进每个算子，算子自己做强转拿到完整的业务对象。
这是一种类型擦除 + 约定强转的设计模式。