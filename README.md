# Incremental Build Graph

一个支持**增量重算**与**缓存失效**的依赖图调度引擎。任务以 DAG 形式声明依赖，
引擎依据稳定指纹决定复用还是重算，缓存持久化到磁盘，并保证并发触发与进程重启后
结果始终一致。

## 核心概念

```
Task ──depends on──▶ Task
  │                    │
  ├─ Input（外部输入）   └─ Artifact（任务产物）
  ├─ Config（配置）
  └─ Run(ctx, input, config, depArtifacts) (Artifact, error)
```

- **Task**：图中的节点，包含 `ID`、`Input`、`Config`、依赖列表 `Deps` 与确定性执行函数 `Run`。
- **Artifact**：任务产物（不透明字节串）。下游任务的指纹按上游产物的 SHA-256 内容哈希计算。
- **Graph**：任务集合。构建完成后应只读地在多次运行之间共享，运行中不要修改。
- **Scheduler**：负责图校验、循环检测、并发调度与缓存读写。

## 快速开始

```go
g := buildgraph.NewGraph()
_ = g.Add(buildgraph.Task{
    ID:    "compile",
    Input: []byte(source),
    Run:   func(ctx context.Context, in, cfg []byte, deps map[string]buildgraph.Artifact) (buildgraph.Artifact, error) {
        return buildgraph.Artifact("obj(" + string(in) + ")"), nil
    },
})
_ = g.Add(buildgraph.Task{
    ID:   "link",
    Deps: []string{"compile"},
    Run: func(ctx context.Context, in, cfg []byte, deps map[string]buildgraph.Artifact) (buildgraph.Artifact, error) {
        return buildgraph.Artifact("bin[" + string(deps["compile"]) + "]"), nil
    },
})

s, _ := buildgraph.New(buildgraph.Options{
    CacheDir:  ".buildcache", // 留空则仅用进程内缓存
    Namespace: "my-project",  // 多图共享同一缓存目录时隔离命名空间
    Workers:   4,             // 0 表示 runtime.NumCPU()
})
res, err := s.Run(context.Background(), g)
// res.Executed：本次真正执行的任务；res.Cached：命中缓存的任务
```

可运行的完整示例见 `example_test.go`。

## 依赖解析与执行规则

1. **校验先于执行**：运行前先检查
   - 每个依赖都指向已注册的任务，否则返回 `unknown task` 错误；
   - 图中不存在环（自依赖也视为环），否则返回 `*CycleError`，其中 `Path` 是
     形如 `[a b c a]` 的闭合环路径，环上的每一条边都真实存在，可直接定位。
2. **检测无副作用**：校验阶段不写缓存、不改图状态。因此即便提交了带环的图，
   已有缓存与图内容也不会被破坏（见 `TestCycleDoesNotCorruptState`）。
3. **拓扑并发调度**：采用 Kahn 算法维护入度，所有依赖都完成的任务进入就绪队列；
   固定大小的 worker 池并发执行互不依赖的任务，任何任务只会在其全部上游产物就绪后启动。
4. **重复依赖去重**：同一依赖在 `Deps` 中出现多次只算一条边，上游任务不会因此执行多次。
5. **确定性契约**：`Run` 必须对相同的 `(Input, Config, 上游产物)` 产生相同产物，
   这是缓存可复用的前提。

## 指纹与缓存命中规则

每个任务的稳定指纹为：

```
SHA256( fingerprintVersion
      ‖ len(Input)  ‖ Input
      ‖ len(Config) ‖ Config
      ‖ for dep in sorted(Deps): len(depID) ‖ depID ‖ SHA256(上游 Artifact) )
```

- 长度前缀编码保证字段内容不会产生歧义；依赖按 ID 排序，指纹与声明顺序无关。
- 命中条件：缓存中存在 `(taskID, fingerprint)` 对应的条目且校验通过。
- 以下任一变化都会改变指纹并导致该任务重算：任务输入、配置、任一上游产物。
- 任务 ID 与命名空间不参与指纹但参与**缓存键**，因此不同任务/不同图之间永不串用。

## 增量失效传播

上游变化沿依赖边向下传播，且只传播到受影响的子图：

1. 上游任务输入/配置变化 → 该任务指纹变化 → 缓存未命中 → 重算；
2. 其产物内容变化 → 所有直接下游的指纹变化（下游自身输入未变也一样失效）→ 级联重算，直到产物恰好不变时传播自然停止；
3. 与变化点不连通的任务指纹不变 → **继续命中，绝不丢弃或重算**。

`RunResult.Executed` / `RunResult.Cached` 可直接用于验证传播范围
（见 `TestInvalidationPropagation`）。

## 持久化与重启复用

- `CacheDir` 指向文件缓存时，每个任务一个条目文件
  （文件名为 `SHA256(namespace ‖ 0x00 ‖ taskID)`），写入采用
  **同目录临时文件 + fsync + 原子 rename**，崩溃时只会留下旧条目或新条目，
  绝不会出现半截目标文件。
- 启动时自动清理上次中断残留的 `.entry-*` 临时文件。
- 因此换一个新进程、新 Scheduler 再次运行，输入不变的任务依然全部命中
  （见 `TestIncrementalHit`，两个 Scheduler 实例模拟重启）。
- 不设置 `CacheDir` 时使用 `MemoryCache`，语义相同但仅在单进程内有效。

### 缓存条目格式与兼容/损坏处理

磁盘格式：

```
"IBGC"(4) ‖ version(1) ‖ fpLen(2) ‖ fingerprint ‖ artifactLen(8) ‖ artifact ‖ SHA256(前面所有字节)(32)
```

读取时依次校验 magic、版本号、长度边界、指纹、尾部校验和以及有无多余字节：

- **版本不兼容**（旧版本或未来版本 magic/version 不匹配）→ 视为未命中，任务被安全重算并覆盖升级；
- **截断、空文件、随机垃圾、校验和不符、长度前缀异常、多余尾部字节** → 一律视为未命中；
- 损坏条目不报错、不影响其他任务；重算成功后原子替换为新的完好条目，后续运行恢复命中。

## 并发一致性

- 对**同一个 `*Graph` 实例**的并发 `Run` 调用会被合并（singleflight）：只执行一次，
  所有调用方拿到同一个 `RunResult`，任务不会重复执行。
- 合并键是图实例身份而非“结构签名”：任务闭包的真实行为无法从结构数据观察，
  两个结构相同但行为不同的图必须各自独立执行（见 `TestConcurrentDifferentGraphsIsolated`）。
- 单个图内部，协调器是唯一的共享状态写者，worker 只在任务派发后读取该任务不可变的
  上游产物；产物先进入内存映射再派发下游，因此不存在部分写入或“缓存与结果不一致”。
- 文件缓存的原子 rename 保证并发/崩溃下也不会读到半成品。
- 全测试套件在 `-race` 下运行通过。

## 失败与中断恢复

- 任务执行返回错误时包装为 `*TaskError{TaskID, Err}`（支持 `errors.Is/As` 解包）；
  运行随即取消，**失败任务的下游永远不会被调度**，失败任务也不会写入任何缓存条目，
  因此下游不可能使用过期产物。
- 与失败任务无关的其他已完成任务条目保留在缓存中。
- 修复后重试：成功产物写入缓存，下游基于新产物重算，整体状态恢复一致；
  再跑一次即全部命中（见 `TestRetryAfterFailureRestoresConsistency`、
  `TestFailedTaskDownstreamNeverUsesStaleOutput`）。
- `context.Context` 取消同样中止调度并返回错误，已在飞行的任务产物被丢弃。

## 验证方法

```bash
# 全部测试（推荐开启 race）
CGO_ENABLED=1 go test ./... -race -v

# 普通运行
go test ./...

go vet ./...
gofmt -l .   # 无输出表示格式干净
```

测试覆盖矩阵：

| 场景                 | 测试                                                |
|----------------------|-----------------------------------------------------|
| 基本拓扑执行         | `TestBasicExecution`                                |
| 增量命中 / 重启复用  | `TestIncrementalHit`                                |
| 失效仅沿下游传播     | `TestInvalidationPropagation`、`TestConfigChangeInvalidates` |
| 指纹稳定与顺序无关   | `TestFingerprintDeterministicAndOrderInsensitive`   |
| 循环依赖可定位       | `TestCycleDetection`、`TestSelfDependencyIsCycle`   |
| 环检测不破坏状态     | `TestCycleDoesNotCorruptState`                      |
| 并发触发结果一致     | `TestConcurrentTriggersCoalesce`                    |
| 无依赖任务并行       | `TestIndependentTasksRunConcurrently`               |
| 不同图并发互不干扰   | `TestConcurrentDifferentGraphsIsolated`             |
| 缓存损坏/截断/垃圾   | `TestCorruptedCacheIsMiss`（5 种损坏形态子用例）     |
| 旧版本缓存识别       | `TestOldVersionEntryIsMiss`                         |
| 命名空间隔离         | `TestCacheNamespaceIsolation`                       |
| 写入中断恢复         | `TestInterruptedWriteRecovery`                      |
| 失败阻断下游         | `TestTaskFailureStopsDownstream`                    |
| 重试恢复一致         | `TestRetryAfterFailureRestoresConsistency`          |
| 下游不得用过旧产物   | `TestFailedTaskDownstreamNeverUsesStaleOutput`      |
| 取消中断             | `TestCancellationAbortsRun`                         |
| 边界情况             | `TestDuplicateDependencyDeclaration`、`TestEmptyGraph`、`TestUnknownDependency` |

## 项目结构

| 文件                 | 职责                                                       |
|----------------------|------------------------------------------------------------|
| `graph.go`           | Task/Graph 数据模型、注册校验                              |
| `cycle.go`           | 迭代式三色 DFS 循环检测与可定位的 `CycleError`             |
| `fingerprint.go`     | 稳定指纹与产物内容哈希                                     |
| `cache.go`           | Cache 接口、文件缓存（版本、校验和、原子写）               |
| `memory_cache.go`    | 进程内缓存实现                                             |
| `scheduler.go`       | 校验、Kahn 并发调度、缓存复用、singleflight 合并、失败处理 |
| `*_test.go`          | 上述全部场景的自动化测试                                   |
