# quotapool

[![Go Reference](https://pkg.go.dev/badge/github.com/laconhub/quotapool.svg)](https://pkg.go.dev/github.com/laconhub/quotapool)
[![Go Report Card](https://goreportcard.com/badge/github.com/laconhub/quotapool)](https://goreportcard.com/report/github.com/laconhub/quotapool)
[![Tests](https://github.com/laconhub/quotapool/actions/workflows/test.yml/badge.svg)](https://github.com/laconhub/quotapool/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**面向第三方 API 厂商配额的分布式限流器 —— 支持原子批量准入。**

[English](README.md) | 简体中文

---

## 为什么需要它

多数限流器只建模一个维度（「每秒 N 次」），而且一次只发一个许可。但真实的厂商配额很少是这个形状：

| 厂商类型 | 限制形状 |
| --- | --- |
| 实时语音厂商 | 限**并发会话数** |
| 云端大模型 API | 限**每分钟请求数** |
| 语音克隆服务 | 限**每秒请求数** |
| 不少厂商 | **同时限多个维度** |

更麻烦的是：当多个服务实例共用同一个厂商账号时，进程内的限流器看不到集群里其他实例已经占掉的配额。

`quotapool` 按资源分别建模**并发**维度（带租约的分布式信号量）与**频率**维度（滑动窗口），并在**单次原子操作中一起准入**。

## 核心能力：原子批量准入

```go
release, err := limiter.Acquire(ctx, "vendor-a", 4) // 四个全拿到，或者一个都不拿
```

`Acquire(ctx, resource, n)` 保证**要么 n 个许可全部拿到，要么一个都不拿**。这不是对 n 次单独申请的便利封装 —— 这就是这个库存在的意义。

当 n 份工作必须**同时启动**时，逐个准入不是性能细节，而是正确性缺陷：先拿到的几个已经开始，剩下的还堵在限流器后面，任何假设「它们同时开始」的调用方都会把这种部分启动观测成卡死或失败。

<details>
<summary><b>一个真实被它规避掉的故障</b></summary>

把同一句台词用四种音色分别合成，再混音成齐声效果。

如果限流器让这四路音色在不同时刻放行，混音器会把晚到的音轨判定为卡死，从而把它们单独播出。听众听到的不是一次齐声，而是同一句话被连着念了四遍 —— 一个完全由限流器准入粒度造成的、用户可见的产品缺陷。

</details>

## 安装

```bash
go get github.com/laconhub/quotapool
```

需要 Go 1.25 及以上。

## 快速开始

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/laconhub/quotapool"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	limiter := quotapool.New(
		quotapool.NewRedisStore(rdb),
		quotapool.WithQuotas(map[string]quotapool.Quota{
			"vendor-a": {Concurrency: 10},                                // 并发会话数
			"vendor-b": {Rate: 160, Window: time.Minute},                 // 每分钟请求数
			"vendor-c": {Rate: 3, Window: time.Second},                   // 每秒请求数
			"vendor-d": {Concurrency: 4, Rate: 100, Window: time.Minute}, // 两个维度都限
		}),
	)

	// 原子地预留四个许可。
	release, err := limiter.Acquire(context.Background(), "vendor-a", 4)
	if err != nil {
		log.Fatal(err)
	}
	defer release()

	// ... 执行那些需要四个许可才能开始的工作 ...
}
```

## 核心概念

### 配额形状

```go
quotapool.Quota{Concurrency: 10}                              // 同时最多 10 个持有者
quotapool.Quota{Rate: 160, Window: time.Minute}               // 每分钟最多准入 160 次
quotapool.Quota{Concurrency: 4, Rate: 3, Window: time.Second} // 两个维度同时生效
quotapool.Quota{}                                             // 不限 —— Acquire 直接放行
```

任一维度取零值即表示该维度不受限。未注册的资源同样不受限，且完全不会访问存储。

配额支持运行时调整，厂商修改账号限额时无需发版：

```go
err := limiter.SetQuota("vendor-a", quotapool.Quota{Concurrency: 20})
```

### 并发许可带租约

并发许可由 `Acquire` 返回的 `release` 函数归还。它同时带有租约（`WithLeaseTTL`，默认 30 秒），这样进程在工作中途崩溃时不会把配额永久占死。

租约时长应明显大于最长的预期持有时间：太短会让仍在运行的工作丢掉许可，太长则会让崩溃的进程占住配额直到租约到期。

频率许可不需要归还 —— 它们会随窗口自然滑出。

### 默认 fail-open

当无法确认配额时 —— 存储不可达、等待预算耗尽、或这批请求根本不可能装下 —— 限流器会**照样放行调用方**，并打一条 warn 日志。

```go
limiter := quotapool.New(store, quotapool.WithFailClosed()) // 反转策略
```

选择标准是：你的系统更能承受哪一种失败？

| 策略 | 风险 | 典型场景 |
| --- | --- | --- |
| fail-open（默认） | 偶尔被厂商侧拒绝一次 | 交互式请求链路 |
| fail-closed | 存储不健康时工作被阻塞 | 计费、反滥用 |

对交互式系统而言，限流器自己挂掉不应该把请求链路一起拖下水。

### 阻塞与非阻塞

```go
release, err := limiter.Acquire(ctx, "vendor", 4)    // 最多等待 WithMaxWait（默认 8 秒）
release, err := limiter.TryAcquire(ctx, "vendor", 4) // 只尝试一次，立即返回
```

等待期间的重试采用带抖动的指数退避。抖动比退避曲线本身更重要：没有抖动时，所有阻塞在同一个耗尽资源上的进程会同步重试，每次唤醒都再次撞在一起，某个 release 释放出的配额会被所有等待者同时争抢。

## 存储后端

| 后端 | 作用范围 | 适用场景 |
| --- | --- | --- |
| `NewRedisStore(client)` | 共用同一存储的所有进程 | 多实例共用一个厂商账号 |
| `NewMemoryStore()` | 单个进程 | 单实例部署、测试 |

`RedisClient` 是一个很窄的接口，`*redis.Client`、`*redis.ClusterClient`、`*redis.Ring` 和 `redis.UniversalClient` 都满足它，可以直接复用你已有的客户端。

也可以自行实现 `Store` 接入其他后端。接口只有两个方法，其中 `Acquire` 的原子性保证是批量准入得以成立的前提。

### Redis 后端的实现方式

两个维度都用按过期时间打分的有序集合存储 —— 并发许可的分值是租约到期时刻，频率许可的分值是准入时刻。每次尝试都会顺带清理已过期的成员。

准入是一段 Lua 脚本，**在写入任何数据之前先检查所有受限维度**。如果把检查和提交拆成两次往返，一批请求可能先消耗掉频率令牌，随后却因并发检查失败而无法使用 —— 这些令牌不会归还，实际频率上限就被悄悄压低了。

键名用哈希标签包裹（`quotapool:{resource}:concurrency`），保证两个键落在同一个 Redis Cluster 槽位。

## 可观测性

```go
stats := limiter.Stats()
// Admitted, Rejected, FailedOpen, FailedClosed, Released, ReleaseFailed
```

`Rejected`/`Admitted` 的比值回答了厂商开始拒绝调用时的第一个问题：**我是不是被限流了？** 而 `ReleaseFailed` 持续上升说明许可正在靠租约过期回收、而不是被主动归还，实际可用配额会低于配置值。

挂上 logger 就能看到每一次失败策略被触发：

```go
limiter := quotapool.New(store, quotapool.WithLogger(slog.Default()))
```

## 配置项

| 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `WithQuotas(map)` | 无 | 按资源配置限额 |
| `WithMaxWait(d)` | `8s` | `Acquire` 的最长阻塞时间 |
| `WithLeaseTTL(d)` | `30s` | 并发许可的租约时长 |
| `WithBackoff(min, max)` | `40ms`、`250ms` | 重试间隔上下界 |
| `WithReleaseTimeout(d)` | `3s` | 归还许可的超时 |
| `WithFailClosed()` | fail-open | 反转失败策略 |
| `WithLogger(l)` | 丢弃 | 用于诊断的 `*slog.Logger` |
| `WithNowFunc(f)` | `time.Now` | 测试中注入时钟 |

## 错误

仅在配置了 `WithFailClosed()` 时返回：

| 错误 | 含义 |
| --- | --- |
| `ErrQuotaExceeded` | 等待预算耗尽仍未拿到配额 |
| `ErrBatchTooLarge` | 批量大小超过某个维度的上限 —— 再等也没用 |
| `ErrInvalidCount` | 批量大小非正数 |
| `ErrInvalidQuota` | `SetQuota` 收到了自相矛盾的限额 |

## 基准测试

`MemoryStore`，Apple M4 Pro，Go 1.25：

```
BenchmarkLimiter_AcquireUnlimited-14        17.50 ns/op      0 B/op    0 allocs/op
BenchmarkLimiter_AcquireBatch/batch-1-14     1397 ns/op    752 B/op   14 allocs/op
BenchmarkLimiter_AcquireBatch/batch-4-14     1375 ns/op    944 B/op   18 allocs/op
BenchmarkLimiter_AcquireBatch/batch-16-14    2786 ns/op   1713 B/op   30 allocs/op
```

未注册的资源会在访问存储之前短路返回。使用 `RedisStore` 时，开销主要由网络往返决定。

## 测试

```bash
make test              # 带 race 检测的单元测试
make test-integration  # 需要一个 Redis 实例
make lint
make benchmark
```

集成测试位于 `integration` 构建标签之后，通过 `QUOTAPOOL_REDIS_ADDR` 指定地址（默认 `localhost:6379`）。连不上服务器时会干净地跳过。

## 常见问题

**并发安全吗？** 安全。`Limiter` 可以被多个 goroutine 同时使用。

**如果 `release()` 一直没被调用会怎样？** 许可会在租约到期时被回收。重复调用 `release()` 是安全的，只有第一次会真正访问存储。

**一个 Redis 能给多个独立的限流器用吗？** 可以，用 `WithKeyPrefix` 给每个限流器分配独立命名空间。

**它能替代 `golang.org/x/time/rate` 吗？** 不能。那个限流器是进程内的、单维度的、单许可的。适用时就用它；当配额跨进程共享、有多个形状、或者必须成批获取时，再用这个。

## 参与贡献

欢迎贡献代码，请参阅 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可证

[Apache License 2.0](LICENSE)
