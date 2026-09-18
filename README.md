# Incremental Build Graph

一个支持**增量重算**与**缓存失效**的依赖图调度引擎。引擎根据任务间的依赖
关系决定执行顺序与结果复用，保证在重复运行、进程重启以及并发触发下结果始终
一致。

## 核心概念

- `Task`：一个计算单元，包含唯一 `ID`、直接依赖 `Deps`、描述自身输入与配置
  的不透明字节串 `Inputs`，以及执行函数 `Run`。
- `Artifact`：任务成功执行后产出的不透明字节负载。
- `Result`：一次运行中某个任务的结论，包含产物、指纹以及是否来自缓存
  （`Cached`）。
- `Engine`：注册任务图并调度目标集合；底层使用可插拔的 `Store` 保存缓存。

```go
e := graph.NewEngine(graph.WithStore(store)) // 默认是内存 Store
e.Register(graph.Task{ID: "a", Inputs: ..., Run: ...})
results, err := e.Run(ctx, "a")
```

## 依赖解析与执行顺序

1. 从本次 `Run` 请求的目标集合出发做可达性分析，只解析目标的**传递依赖闭包**；
   图中无关的任务不会被检查、执行或丢弃。
2. 使用迭代式 DFS 进行校验：
   - 引用了未注册任务时返回包装了 `ErrUnknownTask` 的错误；
   - 遇到回边时报告可定位的依赖环，错误为 `*CycleError`，其 `Cycle` 字段按
     依赖顺序列出环上的任务（自环为单元素）。可用 `graph.AsCycleError(err)`
     提取。
3. 解析阶段**只读不写**，因此未知任务或循环检测失败不会改动任何图状态或缓存。
4. 解析成功后得到拓扑序。执行器在任务的全部依赖完成后立即调度该任务，因此
   **无依赖关系的任务会并发执行**（并发度有上限，避免 goroutine 无界增长）。

## 指纹与缓存命中规则

每个任务的稳定指纹由以下内容做域分离、长度前缀的 SHA-256 计算得到：

- 任务 ID；
- 任务自身的 `Inputs`（输入与配置）；
- 直接依赖指纹的有序集合（依赖按声明顺序参与，并包含依赖数量）。

由于依赖指纹递归地嵌入了各自上游的输入，**任务指纹相等当且仅当该任务及其整条
上游链路上的输入/配置均未变化**。

命中判定（见 `runTask`）：

- 从 `Store` 读取该任务条目；
- 条目的逻辑版本 `Entry.Version` 必须等于当前 `graph.Version`；
- 条目的指纹必须等于当前解析出的指纹。

两者都满足时直接复用缓存产物，标记 `Cached: true`，不调用 `Run`。

### 失效传播

- 任一任务的 `Inputs` 变化 → 其指纹变化 → 该任务缓存未命中并重算；
- 其指纹进入下游指纹 → 受影响的下游任务级联失效，仅这些任务重算；
- 不在受影响下游链路上的任务指纹不变，**继续命中，绝不被丢弃或重算**；
- 重新注册任务（`Register`）不会主动删除缓存；旧条目因指纹不再匹配而被自然
  忽略，并在任务下次成功后被覆盖。

> 注意：指纹只覆盖显式声明的输入、配置与上游产物。若任务读取了未写入
> `Inputs` 的外部状态（如墙上时钟、随机源），引擎无法感知这种变化——调用方
> 应把所有影响输出的输入纳入 `Inputs`。

### 进程重启后的持久命中

`FileStore` 把每个任务的缓存条目以单文件形式存放在目录中（`NewFileStore(dir)`），
写入采用“临时文件 + fsync + 原子 rename”，读者只会看到旧的完整文件或新的完整
文件，不会读到部分写入。使用同一目录重建 `Engine` 即可在重启后继续命中。

### 版本兼容与损坏处理

磁盘格式：`magic "IBGC" | 格式版本(1B) | 长度前缀字段 | SHA-256 校验和`。

- 磁盘格式版本不被识别 → 返回 `ErrCacheVersion`；
- 魔数错误、截断、长度越界、尾部多余字节或校验和不匹配 → 返回
  `ErrCacheCorrupt`；
- 条目的**逻辑版本**（`Entry.Version`）过旧/过新 → 视为未命中。

以上情况引擎一律当作干净的缓存未命中处理：任务重新执行，成功后原子覆盖坏文件
（自愈），不影响正确性。

## 并发语义

- 单次运行内，互不依赖的任务并发执行；任务只会在全部依赖产物就绪后启动。
- 对同一 `Engine` 的多个 `Run` 调用在图粒度上串行化（`runLock`）。在冷缓存上
  同时触发多次相同运行时，任务总共只执行一次；后来的调用观察到先提交的缓存并
  返回命中。所有调用得到的结果一致，不会出现重复执行、部分写入或缓存与结果不
  一致。

## 失败与中断恢复

- 任务执行失败时返回包装了原始错误的 `*TaskError`（含失败任务 ID），可通过
  `errors.As` / `errors.Is` 检查。
- 失败任务的下游任务被标记为跳过，**永不执行**，因此不可能使用过期/部分上游
  产物；本次运行不返回部分结果。
- 失败不会写入新缓存，也不会删除无关任务的缓存条目。
- 修复任务（或其输入）后重试：受影响链路重新计算，成功后整体恢复到一致状态，
  后续运行再次全量命中。
- `context` 取消同样按失败处理：在途任务排空、待执行任务跳过。

## 目录结构

```
graph/
  types.go         公共类型（Task / Artifact / Result / CycleError）
  errors.go        哨兵错误与 TaskError
  engine.go        引擎、注册、运行入口、Validate
  schedule.go      解析（校验/环检测/拓扑/指纹）与并发执行器
  fingerprint.go   稳定指纹
  cache.go         Store 接口与内存实现
  file_store.go    跨进程持久化 Store（原子写入）
  encoding.go      磁盘条目编码/解码（版本 + 校验和）
  *_test.go        全场景自动化测试
```

## 验证方法

```bash
go test ./...                 # 全部测试
go test -race ./...           # 竞态检测（需 CGO_ENABLED=1）
go test -cover ./...          # 覆盖率
go test -run TestCycle ./...  # 只跑循环依赖用例
go vet ./...
```

测试覆盖场景：

| 场景 | 测试 |
| --- | --- |
| 增量命中 | `TestIncrementalHit` |
| 失效传播 / 无关结果保留 | `TestInvalidationPropagation`、`TestUnrelatedResultsUntouched` |
| 循环依赖（含自环、状态不被破坏） | `TestCycleDetection`、`TestSelfCycle`、`TestCycleDoesNotCorruptState` |
| 无依赖任务并发 / 并发触发一致 / 依赖顺序 | `TestIndependentTasksRunConcurrently`、`TestConcurrentTriggersConsistent`、`TestDependencyOrdering` |
| 缓存重启命中 | `TestFileStoreRestartHit` |
| 损坏 / 截断 / 篡改 / 旧版本 | `TestCorruptEntriesAreMisses`、`TestOldVersionEntry`、`TestOldOnDiskFormat` |
| 失败隔离 / 重试恢复 / 过期产物隔离 | `TestFailureDownstreamNotExecuted`、`TestRetryRecovery`、`TestFailureDoesNotInvalidateUnrelatedCache`、`TestFailureDownstreamStaleArtifacts` |
