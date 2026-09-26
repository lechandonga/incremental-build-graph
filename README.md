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
- 该任务**实际消费到的上游产物内容的 SHA-256 哈希**，按声明依赖顺序
  （去重后）参与计算。

指纹在**执行期**根据本次运行中上游实际产出的字节计算，而不是在解析期静态
推导。因此命中判断以任务真正消费到的内容为准：

- 从 `Store` 读取该任务条目；
- 条目的逻辑版本 `Entry.Version` 必须等于当前 `graph.Version`；
- 条目的指纹必须等于按当前上游产物字节算出的指纹。

两者都满足时直接复用缓存产物，标记 `Cached: true`，不调用 `Run`。

### 失效传播

- 任一任务的 `Inputs` 变化 → 其指纹变化 → 该任务缓存未命中并重算；
- **若该任务重算后产出的字节与上次逐字节一致，下游指纹不变，下游继续命中，
  不会被连带重算**——本次运行中只有上游真正执行；
- 若上游产物字节确实变化 → 消费它的下游指纹变化 → 这些下游级联失效并重算，
  绝不会复用到旧产物；
- 不在受影响下游链路上的任务指纹不变，**继续命中，绝不被丢弃或重算**；
- 重新注册任务（`Register`）不会主动删除缓存；旧条目因指纹不再匹配而被自然
  忽略，并在任务下次成功后被覆盖。

> 注意：指纹只覆盖显式声明的输入、配置与实际读取到的上游产物字节。若任务
> 读取了未写入 `Inputs` 的外部状态（如墙上时钟、随机源），引擎无法感知这种
> 变化——调用方应把所有影响输出的输入纳入 `Inputs`。

### 进程重启后的持久命中

`FileStore` 把每个任务的缓存条目以单文件形式存放在目录中（`NewFileStore(dir)`）。
指纹基于产物内容字节，与进程无关，因此上述命中与失效规则在进程重启后完全
成立：使用同一目录重建 `Engine` 即可继续命中。

### 缓存目录的命名与隔离规则

任务 ID 到文件路径的映射是**单射**（`entryRelPath`）：

- 短且字符保守的 ID 使用可读的百分号编码文件名（如 `a.cache`）；
- 其余 ID——含 `/`、`\`、`:`、NUL、`.`/`..`、大写字母（避免大小写不敏感
  文件系统上的碰撞）、Windows 设备名（`nul`、`com1` 等）、超长名称——一律
  走内容寻址的 SHA-256 分片路径（`ab/abcdef….cache`）。

因此**任意合法任务标识**（包括含路径分隔符或层级片段的标识）都能正常写入、
命中、跨重启复用与删除，且永远不会逃逸出缓存目录或与另一个标识碰撞。不同
缓存目录之间严格隔离：一个目录中的条目不可能被另一个目录读取或覆盖。

### 版本兼容、损坏与中断残留

磁盘格式：`magic "IBGC" | 格式版本(1B) | 长度前缀字段 | SHA-256 校验和`。

两类缓存问题在 `Store.Get` 层面可以明确区分：

- **版本不兼容** → 返回包装了 `ErrCacheVersion` 的错误：
  - 磁盘格式版本字节不被识别（校验和完好的前提下）；
  - 条目的逻辑版本 `Entry.Version` 过旧或过新。
- **内容损坏** → 返回包装了 `ErrCacheCorrupt` 的错误：魔数错误、截断、
  长度越界、尾部多余字节、校验和不匹配、指纹字段非法，或文件内嵌的任务 ID
  与读取键不一致（内容不一致）。

引擎对两者一律当作干净的缓存未命中处理：任务重新执行，成功后原子覆盖坏文件
（自愈），不影响正确性；调用方可用 `errors.Is(err, graph.ErrCacheVersion)` /
`errors.Is(err, graph.ErrCacheCorrupt)` 区分失败类别。

**中断残留**：写入采用“唯一临时文件（`.tmp-*`）+ fsync + 原子 rename”，
读者只会看到旧的完整文件或新的完整文件。进程在 rename 前被中断只会留下
`.tmp-*` 孤儿文件，永远不会产生部分可见的条目。`NewFileStore` 打开目录时会
在**排他的跨进程文件锁**（Unix 上为 `flock`）保护下清扫根目录与分片子目录
中的 `.tmp-*` 残留，因此残留既不能影响正常命中，也不会无限堆积；清扫只匹配
临时文件前缀，绝不误删已提交的有效条目。同一目录被多个 `FileStore`（同进程
或跨进程）并发使用时，写入与清扫都经由进程内互斥锁 + 目录文件锁串行化，
保持安全。

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
  schedule.go      解析（校验/环检测/拓扑）与并发执行器、执行期指纹
  fingerprint.go   基于内容哈希的稳定指纹
  cache.go         Store 接口与内存实现
  file_store.go    跨进程持久化 Store（原子写入、残留清扫、目录锁）
  key.go           任务 ID → 文件路径的单射编码（命名与隔离规则）
  lock_unix.go     跨进程目录锁（flock）
  lock_windows.go  目录锁的空实现
  encoding.go      磁盘条目编码/解码（版本 + 校验和 + 字段校验）
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
| 上游重算但字节不变 → 下游保持命中 | `TestUpstreamRecomputedSameBytesDownstreamHits`、`TestContentBasedHitAcrossRestart` |
| 上游字节变化 → 下游必须失效 | `TestUpstreamBytesChangeInvalidatesDownstream` |
| 失效传播 / 无关结果保留 | `TestInvalidationPropagation`、`TestUnrelatedResultsUntouched` |
| 循环依赖（含自环、状态不被破坏） | `TestCycleDetection`、`TestSelfCycle`、`TestCycleDoesNotCorruptState` |
| 无依赖任务并发 / 并发触发一致 / 依赖顺序 | `TestIndependentTasksRunConcurrently`、`TestConcurrentTriggersConsistent`、`TestDependencyOrdering` |
| 缓存重启命中 | `TestFileStoreRestartHit`、`TestRestartInvalidation` |
| 含路径分隔符/层级片段的标识（写入/命中/重启/删除） | `TestPathLikeTaskIDs`、`TestEntryRelPathInjective` |
| 缓存目录严格隔离 | `TestStoreDirectoriesIsolated` |
| 同目录并发写入与并发打开（清扫安全） | `TestConcurrentStoresSameDirectory` |
| 版本不兼容 vs 损坏的区分与自愈 | `TestOldVersionEntry`、`TestOldOnDiskFormat`、`TestCorruptEntriesAreMisses`、`TestBothFailureKindsMissAndSelfHeal`、`TestTaskMismatchIsCorrupt` |
| 中断残留清理且有效条目不受影响 | `TestInterruptedTempSweptKeepsValid` |
| 失败隔离 / 重试恢复 / 过期产物隔离 | `TestFailureDownstreamNotExecuted`、`TestRetryRecovery`、`TestFailureDoesNotInvalidateUnrelatedCache`、`TestFailureDownstreamStaleArtifacts` |
