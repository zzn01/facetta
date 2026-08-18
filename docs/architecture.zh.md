> English version: [architecture.md](architecture.md)

# 实现导览:代码是如何工作的

面向读代码的人的实现走读。README 讲"怎么用",[design-rationale.zh.md](design-rationale.zh.md) 讲"为什么这样设计",
本文讲"代码实际怎么做"。全部源码约 1700 行(不含测试),读完本文再看源码应当没有障碍。

## 一分钟总览

这是一个进程内的只读查询加速层:外部主数据库是唯一事实源,本库把它的一张统计表
镜像成**字典编码的列式内存结构**,支持"多组等值过滤取并集 + 指标求和"这一种查询,
换来微秒级、零堆分配、完全无锁的读路径。

```
                    ┌──────────────────────── Store ────────────────────────┐
                    │                                                        │
   QueryGroups ───▶ │  view.Load() ──▶ ┌──────── view(不可变) ────────┐      │
   (无锁,0 alloc)   │   (原子指针)     │ base: snapshot  排序列式基底  │      │
                    │                  │ delta:          小型未排序覆盖 │      │
                    │                  │ extras:         增量新词典     │      │
                    │                  │ overridden:     基底遮蔽位图   │      │
                    │                  └──────────────────────────────┘      │
   Apply ─────────▶ │  mu ─▶ 整体复制 delta ─▶ 原子换入新 view                │
   Compact ───────▶ │  mu ─▶ base+delta 归并成新 snapshot ─▶ 原子换入          │
   ReplaceAll ────▶ │  mu ─▶ 从全量 dump 重建 ─▶ 原子换入                      │
                    └────────────────────────────────────────────────────────┘
```

三条支柱:

1. **不可变 + 原子切换**。`view` 一经发布绝不修改;所有写操作都构造一个新 `view`
   再 `atomic.Pointer` 换入。读者一次 `Load()` 拿到的整个世界内部自洽,读写互不阻塞。
2. **字典编码列式存储**。维度字符串编码成稠密 `uint32` id,行数据全是数值切片,
   数据区没有任何指针 —— GC 扫描对象数只与字典基数(几万)相关,与行数(百万)无关。
3. **排序基底 + 线性 delta**。基底按 packed key 排序,可二分定位;增量先进小型
   delta 线性扫,周期性归并回基底。经典 LSM 思路的单层内存版。

## 文件地图

| 文件 | 行数 | 职责 |
|------|-----:|------|
| `schema.go` | 161 | `Schema`/`Record`/`Config` 定义与校验,错误哨兵,schema 指纹 |
| `dict.go` | 103 | 值 ↔ uint32 id 字典:字符串维按拼写为键,整数维按 int64 为键(不存字符串) |
| `snapshot.go` | 424 | 不可变列式基底;全量构建(`buildFromRecords`)与归并(`mergeView`/`zipMerge`) |
| `view.go` | 227 | `view`/`delta` 结构;写路径 `applyDelta`(copy-on-write) |
| `store.go` | 193 | `Store` 门面:原子指针、写锁、`Apply`/`Compact`/`ReplaceAll` |
| `query.go` | 470 | `Cond`(等值/IN/范围),共享规划器(`planGroups`),`QueryGroups`,扫描预算 |
| `agg.go` | 179 | `Agg`/`AggOp` 聚合选择,`QueryAggs`(零分配) |
| `groupby.go` | 226 | `QueryGroupBy` 哈希聚合,产出可复用的 `GroupedResult` |
| `compactor.go` | 89 | 可选后台压实策略(何时调 `Compact`) |
| `persist.go` | 272 | 快照落盘/加载,版本化二进制格式 + CRC |
| `stats.go` | 70 | 监控计数器快照 |
| `reference_test.go` | — | 朴素 oracle:查询语义的唯一基准 |
| `equivalence_test.go` | — | 随机负载下引擎 vs oracle 逐位对等 |

## 核心数据结构

### dict(`dict.go`)

`map[string]uint32` + `[]string` 的双向映射,id 按插入顺序稠密分配。约定:**发布进
view 后不可变**,要扩展先 `clone()`。这是全库唯一持有指针(字符串)的地方。

### snapshot(`snapshot.go:13`)

不可变的列式基底,按行主键排序:

```go
dims    [][]uint32    // [维度][行] 字典 id
mets    [][]float64   // [指标][行]
updated []int64       // unix 毫秒,upsert 时间戳
expire  []int64       // unix 毫秒,0 = 永不过期
keys    []uint64      // 每行前 IndexDims 个维度 id 打包成的索引键,升序
```

**packed key**:前 `IndexDims` 个维度的 id 按各自位宽(`dictBits`,由字典基数决定)
左移拼进一个 uint64(`computeShifts`/`packKey`,`snapshot.go:58-83`)。位宽总和超 64
即 `ErrKeyOverflow`。`keys` 升序排列,等值前缀查询就变成两次二分。

**行身份 = 全部维度的组合**;索引键只是前缀,同键的行(索引前缀相同、非索引维不同)
在排序中相邻,靠线性扫区分(`findRow`,`view.go:86`)。所以 `IndexDims` 要选选择性
高的维度放前面,否则查询规划和 upsert 匹配都退化向线性扫。

另外维护三个标量给 O(1) 谓词用:`maxUpdated`(同步位点)、`minUpdated`(TTL 到期
判定)、`minExpire`(行级过期判定,也用于查询时整体跳过 expire 列)。

### delta(`view.go:14`)

新到 upsert 的小型覆盖层:同样列式但**未排序**,查询时无条件线性扫。附带一个
`map[打包id元组]行号` 的索引让同元组的重复 upsert 原地覆盖。维度 id 活在**组合
id 空间**:`id < 基底字典长度` 查基底字典,否则减掉长度查 `extras`。

### view(`view.go:35`)

读者一次 `Load()` 拿到的完整世界:

- `base *snapshot` — 排序基底;
- `delta *delta` — 覆盖层;
- `extras []*dict` — 每维一个"上次压实之后新出现的字符串"字典;
- `overridden []uint64` — 位图,标记"已被某 delta 行遮蔽"的基底行,查询时跳过,
  保证同一逻辑行不被重复计数。

## 读路径:一次 QueryGroups 发生了什么(`query.go:117`)

```
view.Load()                          ── 唯一一次原子读,之后全程操作这个不可变快照
  │
  ├─ 对每组条件 plan()               ── 字符串条件 → 字典 id;找出索引维的
  │    (query.go:31)                    最长全指定前缀,二分出候选区间 [lo,hi)
  │                                     · 值在任何字典都查不到 → dead,该组匹配不了任何行
  │                                     · 值只在 extras 里 → basePossible=false,跳过基底只扫 delta
  │                                     · 无可用前缀 → scan(全基底扫描,计入 Stats.FullScans)
  │
  ├─ 区间并集                        ── ≤16 个区间按 lo 插入排序(栈上数组),
  │                                     用高水位 done 去重,重叠区间只扫一次
  │
  ├─ 扫描预算(可选)                 ── 规划完、未碰任何行时,将要访问的行数已完全可知:
  │                                     去重后的基底候选行 + delta 行数 > MaxScanRows
  │                                     → 裸返回 ErrScanBudget(不 wrap,拒绝路径也零分配)
  │
  ├─ 扫基底候选区间                  ── 每行:overridden 位图跳过 → 过期跳过 →
  │                                     逐组 matchBase,首个命中即累加并 break(并集不重计)
  │
  └─ 线性扫 delta                    ── 同样的过期跳过 + 逐组匹配
```

**零堆分配靠什么**:所有 scratch(`plans`、`ivs`)都是 `maxGroups`/`maxConds` 定长
的栈上数组;`dst` 由调用方复用;错误哨兵预分配。`TestQueryZeroAlloc` 是绊线,
任何热路径改动必须保持它绿。

**过期检查也是按需的**:视图里没有任何可过期行(`base.minExpire==0 && !delta.hasExpiry`)
时,连 `now` 都不采样,expire 列一次都不碰;基底最早过期时刻还在未来时同样整体跳过。
不用行级 TTL 的表为该特性付出的代价是零。一次查询只采样一个 `now`,结果内部一致。

### 另外两个查询入口:QueryAggs 与 QueryGroupBy

三个入口经由 `planGroups`(`query.go`)共享上面的整条流水线:逐组规划、候选区间
取并集、执行扫描预算。区别只在每行的动作 —— `QueryGroups` 对全部指标求和;
`QueryAggs`(`agg.go`)把请求的聚合列折叠进定长的栈上累加器(仍然零分配;
Min/Max 从首个命中行初始化,Count/Avg 派生自共享的行计数器 —— `AggDistinct` 列
是例外:每列分配一张按该维度合并基数定尺寸的 id 位图,以测试并置位(test-and-set)
计数新 id,结果精确且无需 popcount 遍历);`QueryGroupBy`
(`groupby.go`)哈希聚合进可复用的 `GroupedResult`:分组键是 by 维度的打包 id
元组(在同一 view 内、跨 base 与 delta,id 唯一标识字符串),map 查找用不分配的
`m[string(bytes)]` 形式,分组由其首行直接创建(无需哨兵初始化),输出按键字符串
排序以保证确定性。group-by 中的 distinct 列经由挂在结果上的一个共享
seen-(列, 组, id) 集合逐组去重。它的分配是每次调用 O(结果组数),并在复用的结果上摊销 ——
这是零分配规则唯一的文档化例外。

范围条件(`Cond.Range`,仅限整数维)搭载在同一套机制上。在声明为 `DimInt`
(`Dim.Type`)的维度上,身份就是 int64 值,而其字典在结构上强制这一点:字典
直接以 int64 本身为键(`map[int64]id` 外加一个 `vals []int64` 列),完全不存
字符串。写入与查询边界只做 ParseInt 再查值(非整数输入报错拒绝);这些路径上
从不渲染任何拼写,因此同一个整数的不同拼写根本无法作为独立词条存在。查询时
一次范围检查就是一次 id 查找加两次整数比较 —— 不扫字典、不用位图、零分配,
维度身份中也不存在任何浮点精度悬崖。拼写只在输出边界按需渲染:group-by 键
(按调用渲染,落在文档化的 O(结果组数) 分配预算内)和快照保存。快照格式不受
影响;加载时把词条解析回来(非整数词条会拒绝该快照,同一整数的多个拼写坍缩
为一个 id 会触发条目计数校验),因此在该维声明为 DimInt 之前写出的快照会被
拒绝,进入回退全量拉取。

IN 条件(`Cond.In`)被解析进按查询一份的 id 池(`queryIns`)而不是 `groupPlan`,
并由调用点的 `matchIns` 检查,而不是放进 `matchBase`/`matchDelta` 内部。两处摆放
都是刻意的热路径保护,合并前经过实测:按 plan 一份的 id 数组会让 plans 的栈
scratch 膨胀一个数量级(而它们每次查询都要全量清零初始化);把 IN 检查折进匹配器
会把它们推出编译器的内联预算 —— 两者都会让索引查询付出两位数百分比的代价。
不含 IN 的查询传入 nil 池,每个命中行只多付一次永不成立的分支。

## 写路径:Apply 的 copy-on-write(`view.go:113`)

`Apply` 持 `mu`,对当前 view 做**整体复制再修改**:delta 各列 append 复制、元组索引
`maps.Clone`(行序不变所以旧索引直接可用,不必逐行重编码)、`extras` 按维**惰性**
clone(本批次没给该维添新词就直接共享旧字典 —— 未变的字典不可变,跨 view 共享安全)。

每条记录的 upsert 语义(与 oracle 逐条对齐):

1. `UpdatedAt` 早于 TTL 截止线 → 直接丢;
2. 元组已在 delta 索引 → 时间戳不更旧就**整行替换**(含 `ExpireAt`),更旧则忽略;
3. 元组不在 delta、所有 id 都在基底字典 → `findRow` 查基底:基底行严格更新则忽略
   (乱序旧记录),否则追加进 delta 并置 `overridden` 位遮蔽基底行;
4. 已过期(`ExpireAt <= now`)的记录**照常存入**,靠读时跳过隐藏 —— 先存后滤,
   保证去重顺序与 oracle 逐位一致,物理回收留给下次归并。

一次 `Apply` 的成本是 O(现存 delta 行数 + 批大小),所以宿主要**攒批**调用,并用
`CompactorConfig.MaxDeltaRows`/`DeltaRatio` 限制常驻 delta 把单次成本封顶。

## Compaction:归并与字典压实(`store.go:140`,`snapshot.go:229`)

`Compact` = minor compaction。持 `mu`(与 Apply/ReplaceAll 互斥,**查询不受影响**),
把 base+delta 归并成全新 snapshot 后原子换入,失败(超行数上限/键溢出)则旧视图
继续服务。

核心是 `zipMerge`(`snapshot.go:300`)—— 一次线性双路归并:

1. delta 行按新 packed key 排序(基底本来就有序);
2. 双指针 zip,边走边丢三类行:被 `overridden` 遮蔽的基底行、`UpdatedAt` 过了
   全局 TTL 的行、`ExpireAt` 已到的行(物理回收);
3. 产出仍按 key 有序的新 snapshot,并重算 max/min 标量。

**字典压实**(`mergeView` 的 `renumber` 参数)按 `Config.DictCompactInterval` 门控:

- **id 稳定路径**(窗口内):新字典 = 基底字典 + extras 追加,id 不变,行数据原样
  搬运;extras 为空的维直接共享基底字典对象,shifts 未变时基底 keys 列也原样复用。
- **压实路径**(窗口到期):标记遍在组合 id 空间记录存活行引用了哪些 id → 幸存词条
  按 id 序单调重编号(单调 ⇒ 基底排序不被破坏,归并仍是同一个线性 zip)→ 按活跃
  基数重算位宽。把被淘汰行留下的"幽灵词条"和位宽虚高一并回收。

  重编号两遍约占归并耗时的 40–60%(88ms vs 52ms @1M),这就是门控存在的理由。

**溢出自愈**:id 稳定归并报 `ErrKeyOverflow` 时,无视门控立即带字典压实重试一次
(`store.go:148`)—— 位宽是按含垃圾的字典长度算的,压实后往往就装得下。只有**活跃**
基数真超 64 bit 才持续拒绝(旧视图继续服务,等 TTL/墓碑收缩后自愈)。

`ReplaceAll` = major compaction / 对账:从全量 dump 走 `buildFromRecords`
(`snapshot.go:105`)整体重建 —— 编码、排序(同元组新者在前)、去重、丢弃已过期的
幸存者,字典天然全新。这是**唯一**能收敛"绕过墓碑的上游硬删除"的路径,节奏由宿主
按漂移容忍度决定。

`NeedsCompaction`(`store.go:59`)是 O(1) 无锁谓词:delta 非空 ∥ `minUpdated` 过了
全局 TTL ∥ `minExpire` 已到。Compactor 的周期 tick 用它兜底,保证摄入空闲时被读时
跳过的过期行也能被物理回收。

## Compactor:策略与机制分离(`compactor.go`)

`Store` 只提供机制,`Compactor` 是可选的默认策略,对数据来源一无所知:

- 每 `CheckInterval`(默认 10s):delta/base 超 `DeltaRatio`(相对界,默认 0.1)或
  delta 行数达 `MaxDeltaRows`(绝对界,约束单查询扫描成本)→ 压实。cap-blocked 时
  跳过 —— 重试只会再次撞上限;
- 每 `CompactInterval`(默认 5m):`NeedsCompaction()` 成立 → 压实。cap-blocked 时
  **仍尝试**,这是超限后的恢复探测(TTL 收缩/上游删除可能已让表装得下);
- 压实成功且配置了 `SnapshotPath` → 顺手落盘。

## 生命周期:TTL、墓碑与"复活"边界

两套过期机制正交叠加,任一判死即淘汰:

| | 全局 `Config.TTL` | 行级 `Record.ExpireAt` |
|---|---|---|
| 基准 | `UpdatedAt`(保留窗口) | 绝对时刻 |
| 不可见时机 | 下次归并才消失 | `now >= ExpireAt` 读时立即不可见 |
| 物理回收 | 归并/重建时 | 归并/重建时 |

上游没有删除事件,软删除 = 写一条 `ExpireAt <= now` 的记录(墓碑):立即不可见,
下次归并物理回收。**已定义的边界**:物理回收会忘掉该元组的 `UpdatedAt` 水位,之后
乱序到达的**更旧**记录会复活该行 —— 永久记住所有墓碑会无界耗内存,故意不做;oracle
的 `reclaimExpired` 同步建模了这一点,此类漂移与硬删除漂移同属 `ReplaceAll` 修复。

`SyncPosition`(宿主增量拉取的续传位点)= 可见行的 max `UpdatedAt`,归并回收掉最新
行后**可能回退**;宿主重拉只会重新摄入已过期记录,它们保持不可见并在下次归并再丢,
无可观察不一致(`store.go:39` 注释)。

## 持久化(`persist.go`)

用途只有一个:重启加速。格式 v2,小端二进制:

```
"OLSNAP01" | version(u32) | schema指纹(u64) | maxUpdated(u64) | 行数(u64)
| 各维字典(条数 + 逐串) | dims 各列 | mets 各列 | updated 列 | expire 列 | CRC32C
```

- **只存基底**:delta 行都比落盘的同步位点新,重启后增量重拉自然补回;
- **原子写**:临时文件 + fsync + rename;
- **加载即校验**:CRC、magic、版本、schema 指纹、id 越界、keys 有序性、maxUpdated
  与逐行重算一致 —— 任何一项不过就整体拒绝,store 不动,宿主回退全量拉取。位点
  早于 TTL 截止线的陈旧快照同样拒绝(`errSnapshotStale`);
- **不做跨版本兼容**:版本不符直接拒绝(回退全量拉取是设计行为),换取格式代码
  极简。改动列/头必须递增 `snapVersion`。

## 并发模型

- **读者**:一次 `view.Load()`,之后全程无锁。旧 view 被换出后仍被在途查询持有,
  由 GC 自然回收 —— 不可变性使"epoch/引用计数"这类机制完全不需要。
- **写者**:`mu` 串行化 Apply/Compact/ReplaceAll/LoadSnapshot。Compact 全程持锁
  (5M 行约几百 ms),期间 Apply 会停等 —— 这是文档化的取舍,换来的是查询永不被
  写阻塞。
- **计数器**:全部 `atomic`,`capBlocked` 也是 atomic 以便 Apply 之外(Compactor
  的无锁探测)读取。

## 自我保护

- **`MaxRows`**:归并/重建产物超限 → 拒绝换入、旧视图继续服务、置 `capBlocked`。
  期间 `Apply` 丢弃"会创建新行"的记录(对既有行的更新仍然放行,计入
  `Stats.DroppedOverCap`),防止 delta 无界膨胀;某次归并重新装得下即自动解除。
- **`MaxScanRows`**:见读路径。把 O(N) 最坏情况变成即时拒绝(`ErrScanBudget` =
  "这层太贵,快速回退主库"),约束的是**工作量**(访问行数)不是墙钟。
- **字典/位宽水位**:`Stats.DictEntries`、`Stats.IndexKeyBits`(现在压实的话索引
  键需要多少 bit,预算 64,建议 >56 告警)。

## 正确性如何保证

- **oracle 先行**(`reference_test.go`):每行一个堆对象 + 线性扫的朴素实现是查询
  语义的**唯一基准**。改引擎语义必须同步改 oracle。
- **随机等价**(`equivalence_test.go`):同一随机负载(乱序时间戳、三态 ExpireAt、
  Apply/Compact/ReplaceAll 混合、注入时钟中途推进、奇数种子门控字典压实以覆盖两条
  归并路径)同时驱动引擎与 oracle,每步 20 个随机查询逐位比对。
- **绊线**:`TestQueryZeroAlloc`(热路径 0 alloc)、`TestCapacity5M`(5M 行内存/
  延迟红线)、`TestQueryLatencyDuringCompact`(读不被写阻塞)、`TestStressScale`
  (按需规模压测,`FACETTA_STRESS_ROWS` 门控)。

## 宿主集成的完整图景

库边界 = 纯存储 + 压实策略;下面这些**都是宿主的活**,不要加回库里:

```
启动:  LoadSnapshot(path) 成功 ──▶ 从 SyncPosition() 续传增量
        失败(缺失/损坏/版本不符/陈旧) ──▶ ReplaceAll(全量拉取)

常态:  循环 fetchSince(SyncPosition()) ──▶ Apply(攒批)
        go compactor.Run(ctx)              (后台归并 + 落盘)

对账:  周期 / 手动 ReplaceAll(全量)       (唯一的漂移修复路径)
```
