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

## 命中与失效判定（基于实际消费内容）

命中只取决于**任务实际消费到的内容**，而不是上游是否被重新执行。每个任务的
**内容指纹**由以下成分做域分离、长度前缀的 SHA-256 得到：

- 任务 ID；
- 任务自身的 `Inputs`（输入与配置）；
- 任务上一次成功执行时**实际读取过的直接依赖产物**：按首次读取顺序记录，
  每项包含依赖 ID 与该次读取拿到的产物**字节的哈希**。

因此：

- 上游的输入/配置变了、上游也确实重算了，但只要它这次产出的字节与上次**逐字节
  一致**，下游重放出来的内容指纹就不变 → **下游继续命中、绝不重算**；
- 上游产物字节一旦变化，所有**读取过**该产物的下游指纹必然变化 → **全部失效重算，
  绝不复用旧产物**；
- 声明了却没有读取的依赖，其变化不会牵连本任务。

未读取任何依赖的任务，其指纹仅由 ID 与自身 Inputs 决定。

### 指纹的产生与重放

- 解析（`resolve`）只做校验、环检测与拓扑排序，**不再**预先计算指纹——因为指纹
  依赖运行时才能拿到的上游产物字节。
- 任务执行时，`Inputs.Artifact` 会记录每个被读取的**已声明**依赖（ID + 字节哈希，
  重复读取只记一次）。执行成功后用这些记录与当前 Inputs 算出内容指纹，连同
  “消费清单”（`Entry.Consumed`）一起原子写入缓存。
- 下次运行命中判定：读出条目，按其记录的消费清单从**当前**依赖产物字节重算哈希与
  内容指纹；条目版本受支持且指纹一致才复用，标记 `Cached: true`，否则执行任务。

该判定完全基于持久化内容，因此**进程重启后同样成立**：上游因配置变化而重算、但
产物字节不变时，下游在新进程里依旧命中磁盘条目。

> 注意：指纹只覆盖显式声明的 `Inputs` 与任务**实际读取**的上游产物字节。若任务的
> 产物受未写入 `Inputs` 的外部状态影响（墙上时钟、随机源、隐式文件等），引擎无法
> 感知这类变化——调用方应把所有影响输出的输入纳入 `Inputs`。

### 进程重启后的持久命中

`FileStore` 把每个任务的缓存条目以单文件形式存放在目录中（`NewFileStore(dir)`），
写入采用“加目录锁 → 清理中断残留 → 临时文件 + fsync + 原子 rename”，读者只会看到
旧的完整文件或新的完整文件，不会读到部分写入。使用同一目录重建 `Engine` 即可在
重启后继续命中。

### 任务标识到文件名：任意合法 ID、平面、无碰撞、目录隔离

任务 ID 不会被直接拼进路径，而是映射为**仅含十六进制字符与固定标点**的平面
文件名（`entryKey`），并保证生成的文件名**长度永不超过 255 字节**（ext4、
xfs、tmpfs、APFS、NTFS 等常见本地文件系统的单段名上限 `NAME_MAX`）：

- 长度 **≤ 124 字节**的 ID：对原始字节做十六进制编码（在 ID 集合上是双射，
  **绝不碰撞**），再加固定后缀 `.cache`。阈值由 `2×len(id)+len(".cache") ≤ 255`
  推导（`directKeyLimit`），所以即使最长的直接编码名也只有 254 字节；
- 长度 **≥ 125 字节**的 ID（即中等长度与超长 ID）：改用域分离前缀
  `"task:" + id` 的 SHA-256，文件名恒为 72 字节，形如 `h-<64hex>.cache`
  （`h-` 前缀是十六进制直接编码名不可能出现的字符，两个命名空间天然分离；
  SHA-256 的碰撞概率可忽略）。

> 历史问题：旧版本阈值取 200，于是 **125～200 字节**的 ID 仍走直接编码，
> 生成 256～406 字节的文件名，在开启文件缓存时直接 `ENAMETOOLONG`、整次
> 运行中断；更短或超过 200 字节的 ID 反而正常。阈值改为按 255 字节上限推导
> 后，**任意长度**的合法标识都能正常写入、命中、跨重启复用与删除。

因此 ID 的**字节长度没有任何限制**，其中包含路径分隔符（`/`、`\`）、
`.`/`..` 层级片段、前导分隔符、空格、Unicode、NUL 之外的任意字节甚至空串都
完全合法：可正常写入、命中、跨重启复用与删除，且文件始终直接落在缓存目录内，
**不会创建子目录，也不可能逃逸或覆盖目录外的文件**。

**目录隔离**：文件名只与 ID 字节有关、不含任何目录前缀；不同缓存目录之间严格
隔离——一个目录中的条目不会被另一个目录读到或覆盖，在两个目录中写入同一 ID
也互不影响。（要求缓存目录所在文件系统支持至少 255 字节的单段文件名，即符合
POSIX `NAME_MAX=255` 的常见本地文件系统。）

### 版本兼容、内容损坏与中断残留

磁盘格式（v2）：

```
magic "IBGC" | 格式版本(1B)
| len前缀字段 × 5：逻辑版本(u32)、任务ID、内容指纹、产物字节、消费清单
| SHA-256(前面全部字节)
```

其中“消费清单”为 `count(uvarint) | (任务ID len前缀 | 产物哈希 len前缀)*count`，
记录任务实际读取的上游产物字节哈希。

两类失败被**明确区分**（调用方可 `errors.Is` 判断），引擎都按干净的未命中处理，
任务重算成功后原子覆盖坏文件（自愈）：

- `ErrCacheVersion`：无法识别的磁盘格式版本字节，或结构完好但**逻辑版本**
  过旧/过新（`Entry.Version != graph.Version`）；
- `ErrCacheCorrupt`：魔数错误、截断、长度越界、尾部多余字节、校验和不匹配、
  指纹/哈希非十六进制或长度不符，以及条目内容与其所在键不一致等。

中断写入会留下 `.tmp-*` 临时文件：

- 临时文件永远不会被当作条目读取（命名不同），所以不影响正常命中；
- 复用目录（`NewFileStore`）以及每次 `Put` 前，会在持有**目录锁**时清扫
  `.tmp-*` **普通文件**，因此残留不会无限堆积；写入自身若在 rename 前任一步
  失败（写盘、fsync、关闭出错，包括旧版本里目标文件名超长导致的 rename
  失败），会立即删除自己的临时文件，**不会留下半写文件**，也不会触碰同目录
  里其它任务已经有效的条目；
- 清扫只匹配此前缀、只删普通文件、不递归，**绝不会误删任何有效条目**或锁文件；
  命名相似的目录/符号链接会被保留；
- 目录锁（Unix 上为对 `.lock` 文件的 `flock`，进程崩溃后由内核自动释放）保证
  同一目录被多个进程/实例并发使用时，清理与写入彼此串行、仍然安全；并发写入
  各自使用独立临时文件、目标名由 ID 唯一确定，因此互不覆盖；rename 的原子性
  保证任何读者都不会读到不完整内容。

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
  types.go             公共类型（Task / Artifact / Result / CycleError）、逻辑版本
  errors.go            哨兵错误（含 ErrCacheVersion / ErrCacheCorrupt）与 TaskError
  engine.go            引擎、注册、运行入口、Validate
  schedule.go          解析（校验/环检测/拓扑）与并发执行器、内容指纹命中与消费记录
  fingerprint.go       基于自身输入与实际消费产物字节的内容指纹
  cache.go             Store 接口、Entry/ConsumedDep 与内存实现
  file_store.go        跨进程持久化 Store（平面键名、目录锁、残留清扫、原子写入）
  encoding.go          磁盘条目编解码（格式版本 + 消费清单 + 校验和）
  dirlock_unix.go      Unix 目录锁（flock）
  dirlock_windows.go   Windows 下的锁占位
  *_test.go            全场景自动化测试
```

## 验证方法

```bash
go test ./...                              # 全部测试
CGO_ENABLED=1 go test -race ./...          # 竞态检测（需 CGO）
go test -cover ./...                       # 覆盖率
go test -run TestUpstream ./...            # 只跑“上游重算/字节变化”相关用例
go test -run TestMedium ./...              # 只跑中等/任意长度层级 ID 的修复回归用例
go vet ./...
```

测试覆盖场景：

| 场景 | 测试 |
| --- | --- |
| 增量命中 | `TestIncrementalHit` |
| 失效传播 / 无关结果保留 | `TestInvalidationPropagation`、`TestUnrelatedResultsUntouched` |
| 上游重算但字节不变→下游命中且只有上游执行 | `TestUpstreamRecomputeSameBytesDownstreamHit` |
| 上游字节变→下游失效、不用旧产物（含跨重启） | `TestUpstreamBytesChangeInvalidatesDownstream`、`TestUpstreamBytesChangeAcrossRestart`、`TestUpstreamSameBytesDownstreamHitAcrossRestart` |
| 只有实际读取的依赖影响命中 | `TestOnlyConsumedUpstreamsMatter` |
| 任意 ID（分隔符/层级/空/超长）写入、跨重启、删除 | `TestArbitraryTaskIDsRoundTrip`、`TestArbitraryTaskIDsAcrossRestart`、`TestWeirdIDsThroughEngine`、`TestEntryKeysAreCollisionFree` |
| 中等长度（125～200B，旧版本必崩区间）及任意长度层级 ID 的写入/命中/跨重启/删除、键长≤255 与无碰撞、不干扰他人条目、引擎级跨重启、同目录并发 | `TestMediumHierarchicalTaskIDsRoundTrip`、`TestMediumIDsIsolatedAcrossDirectories`、`TestMediumIDsDoNotDisturbOtherEntries`、`TestMediumHierarchicalIDThroughEngine`、`TestMediumIDsConcurrentSameDirectory`、`TestMediumEntryKeysAreFlatCollisionFreeAndBounded` |
| 缓存目录严格隔离 | `TestCacheDirectoriesAreIsolated` |
| 版本不兼容 vs 内容损坏被区分、皆为未命中且自愈 | `TestVersionAndCorruptionDistinguished`、`TestOldVersionEntry`、`TestOldOnDiskFormat`、`TestCorruptEntriesAreMisses` |
| 中断残留清理且有效条目不受影响 / 并发同目录安全 | `TestOrphanTempFilesCleanedOnReuse`、`TestLockSerializesCleanup`、`TestConcurrentFileStoresSameDirectory` |
| 循环依赖（含自环、状态不被破坏） | `TestCycleDetection`、`TestSelfCycle`、`TestCycleDoesNotCorruptState` |
| 无依赖任务并发 / 并发触发一致 / 依赖顺序 | `TestIndependentTasksRunConcurrently`、`TestConcurrentTriggersConsistent`、`TestDependencyOrdering` |
| 缓存重启命中 | `TestFileStoreRestartHit`、`TestRestartInvalidation` |
| 失败隔离 / 重试恢复 / 过期产物隔离 | `TestFailureDownstreamNotExecuted`、`TestRetryRecovery`、`TestFailureDoesNotInvalidateUnrelatedCache`、`TestFailureDownstreamStaleArtifacts` |
