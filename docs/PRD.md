# TianShu.MQ — 本地高性能消息队列 PRD

## 1. 概述

### 1.1 产品定位

TianShu.MQ 是一个面向 .NET 生态的**本地嵌入式消息队列** NuGet 类库。它运行在应用程序进程内，无需独立部署的 Broker 进程，提供类似 Kafka 的主题（Topic）和分区（Partition）模型，支持内存和持久化两种存储模式，目标是在本地场景下提供极致性能。

### 1.2 核心目标

| 目标 | 说明 |
|------|------|
| **高性能** | 内存模式吞吐量 ≥ 500,000 msg/s，持久化模式 ≥ 100,000 msg/s（单机） |
| **低延迟** | P99 延迟 < 1ms（内存模式），< 5ms（持久化模式） |
| **易集成** | WebAPI / 微服务项目只需引入 NuGet 包，一行代码即可启动 |
| **分区模型** | 借鉴 Kafka 分区设计，支持分区顺序、并行消费 |
| **可靠性** | 持久化模式下消息不丢失，支持 ACK 机制和故障恢复 |

### 1.3 非目标

- 不做跨进程网络分布式消息队列（非 Kafka / RabbitMQ 替代品）
- 不做跨机器消息复制（不内置 Raft / Gossip 协议）
- 不做管理控制台 UI

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                     Application (WebAPI)                │
│  ┌──────────────────────────────────────────────────┐   │
│  │              TianShu.MQ (In-Process)             │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐       │   │
│  │  │ Producer │  │ Consumer │  │  Admin   │       │   │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘       │   │
│  │       │              │             │              │   │
│  │  ┌────▼──────────────▼─────────────▼──────────┐   │   │
│  │  │           MessageRouter (核心路由)           │   │   │
│  │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐   │   │   │
│  │  │  │  Topic 1 │ │  Topic 2 │ │  Topic N │   │   │   │
│  │  │  │ P1 P2 P3 │ │ P1 P2    │ │ P1 P2 P3 │   │   │   │
│  │  │  └──────────┘ └──────────┘ └──────────┘   │   │   │
│  │  └───────────────────┬────────────────────────┘   │   │
│  │                      │                             │   │
│  │  ┌───────────────────▼────────────────────────┐   │   │
│  │  │          Storage Engine 存储引擎             │   │   │
│  │  │  ┌────────────────┐  ┌────────────────┐    │   │   │
│  │  │  │  MemoryStorage │  │  PersistStorage │    │   │   │
│  │  │  │  (Channel +    │  │  (RocksDB /     │    │   │   │
│  │  │  │   RingBuffer)  │  │   Custom File)  │    │   │   │
│  │  │  └────────────────┘  └────────────────┘    │   │   │
│  │  └─────────────────────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### 2.2 分区模型（Partition）

分区是 TianShu.MQ 的核心设计概念，与 Kafka 保持一致：

| 概念 | Kafka | TianShu.MQ |
|------|-------|------------|
| Topic | ✅ | ✅ |
| Partition | ✅ | ✅ |
| Offset | ✅ | ✅ |
| Consumer Group | ✅ | ✅ |
| 分区内有序 | ✅ | ✅ |
| 全局有序 | ❌ | ❌ |

**分区设计要点：**

- 一个 Topic 可包含多个 Partition，Partition 数量在创建 Topic 时指定
- 每个 Partition 内部消息严格有序（FIFO），Offset 单调递增
- 不同 Partition 之间可以并行生产和消费
- Producer 通过 `PartitionKey` 决定消息进入哪个 Partition（`hash(key) % N`）
- 分区数支持运行时动态扩容（但已有消息不重新分布）

### 2.3 消费者组（Consumer Group）

- 同一 Consumer Group 内的多个消费者实例，每个 Partition 最多被一个消费者消费
- 一个消费者可以消费多个 Partition
- 不同 Consumer Group 之间独立消费，互不影响（发布-订阅模式）
- 支持手动 ACK（At-Least-Once）和自动 ACK（At-Most-Once）

---

## 3. 核心功能

### 3.1 消息模型

```csharp
public class Message<T>
{
    /// <summary>消息唯一 ID (GUID)</summary>
    public string MessageId { get; }
    /// <summary>消息体</summary>
    public T Body { get; }
    /// <summary>分区键（决定进入哪个分区）</summary>
    public string PartitionKey { get; }
    /// <summary>消息偏移量（分区内自增）</summary>
    public long Offset { get; internal set; }
    /// <summary>消息头（自定义元数据）</summary>
    public IDictionary<string, string> Headers { get; }
    /// <summary>消息时间戳</summary>
    public DateTime Timestamp { get; }
    /// <summary>延迟投递时间（到期后可见）</summary>
    public DateTime? ScheduledEnqueueTime { get; }
}
```

### 3.2 生产消息

```
// 基础发布
await producer.PublishAsync("order-events", new OrderCreatedEvent { ... });

// 指定分区键（同 key 进入同分区，保证顺序）
await producer.PublishAsync("order-events", order.OrderId, new OrderCreatedEvent { ... });

// 批量发布（高性能）
await producer.PublishBatchAsync("order-events", batchMessages);

// 延迟消息
await producer.PublishAsync("order-events", message, scheduledTime: DateTime.Now.AddMinutes(30));
```

### 3.3 消费消息

```
// 推送模式（Push - 回调）
consumer.Subscribe("order-events", "payment-group", async (Message<OrderEvent> msg) =>
{
    await processOrder(msg.Body);
    return ConsumeResult.Success;
});

// 拉取模式（Pull）
var messages = consumer.PullAsync("order-events", "payment-group", maxBatchSize: 100);

// 手动 ACK
consumer.Subscribe("order-events", "payment-group", async (msg, ack) =>
{
    await processOrder(msg.Body);
    await ack.CommitAsync(); // 完成处理后手动确认
});
```

### 3.4 两种存储模式

| 特性 | MemoryStorage | PersistStorage |
|------|--------------|---------------|
| 存储介质 | 内存 RingBuffer | RocksDB / 本地文件 |
| 持久化 | ❌ 进程重启丢失 | ✅ 文件持久化，重启恢复 |
| 吞吐量 | ≥ 500,000 msg/s | ≥ 100,000 msg/s |
| P99 延迟 | < 1μs | < 5ms |
| 容量上限 | 受内存限制 | 受磁盘限制 |
| 适用场景 | 缓存、实时流、临时事件 | 订单、关键业务事件 |

**持久化存储实现方案：**
- 主选：**RocksDB**（LSM-Tree，写入吞吐极高，嵌入式）
- 备选：自定义文件追加日志（`.index` + `.log` 文件格式，类似 Kafka 的 Segment）
- 消息刷盘策略：`FlushInterval` 定时刷盘 / `FlushPerMessage` 每次写入刷盘

### 3.5 管理 API

```
// 创建 Topic（指定分区数、存储模式）
admin.CreateTopicAsync("order-events", partitions: 3, storageMode: StorageMode.Persist);
admin.CreateTopicAsync("real-time-events", partitions: 6, storageMode: StorageMode.Memory);

// 获取 Topic 状态
var stats = admin.GetTopicStats("order-events");
// → { PartitionCount, TotalMessages, TotalBytes, EachPartitionOffsetRange }

// 查看消费组进度
var progress = admin.GetConsumerGroupProgress("order-events", "payment-group");
// → { EachPartition: { CurrentOffset, Lag } }

// 重置消费偏移（回溯消费）
admin.ResetOffsetAsync("order-events", "payment-group", OffsetReset.Earliest);
admin.ResetOffsetAsync("order-events", "payment-group", OffsetReset.ToTimestamp(DateTime.Now.AddHours(-1)));
```

---

## 4. 集成方式（WebAPI）

### 4.1 NuGet 包

| 包名 | 说明 |
|------|------|
| `TianShu.MQ` | 核心库（内存模式） |
| `TianShu.MQ.Persist` | 持久化扩展（RocksDB 存储） |
| `TianShu.MQ.AspNetCore` | ASP.NET Core 集成包（DI + 配置） |

### 4.2 WebAPI 集成示例

**Step 1 — 安装 NuGet 包**

```bash
dotnet add package TianShu.MQ
dotnet add package TianShu.MQ.AspNetCore
```

**Step 2 — 注册服务**

```csharp
// Program.cs
builder.Services.AddTianShuMQ(mq =>
{
    mq.AddTopic("order-events", options =>
    {
        options.Partitions = 3;
        options.Storage = StorageMode.Persist;
    });

    mq.AddTopic("real-time-alerts", options =>
    {
        options.Partitions = 6;
        options.Storage = StorageMode.Memory;
        options.MemoryCapacity = 1024 * 1024; // 最多 100 万条
    });
});
```

**Step 3 — 注入生产/消费**

```csharp
[ApiController]
public class OrderController : ControllerBase
{
    private readonly IMessageProducer _producer;

    public OrderController(IMessageProducer producer)
    {
        _producer = producer;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder(OrderDto dto)
    {
        // 按订单 ID 分区，保证同一订单的事件顺序
        await _producer.PublishAsync("order-events", dto.OrderId, dto);
        return Ok();
    }
}

// Background Service 消费
public class OrderConsumerService : BackgroundService
{
    private readonly IMessageConsumer _consumer;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _consumer.SubscribeAsync("order-events", "payment-group",
            async msg => await HandleOrder(msg.Body));
    }
}
```

---

## 5. 性能设计

### 5.1 关键技术选型

| 模块 | 技术方案 | 选型理由 |
|------|---------|---------|
| 内存队列 | `System.Threading.Channels` + RingBuffer | .NET 内置无锁并发，极低开销 |
| 序列化 | `MemoryPack` / `MessagePack` | 二进制序列化，比 JSON 快 5-10 倍 |
| 持久化存储 | RocksDB (原生 LSM-Tree) | 写入吞吐极高，嵌入式无需外部依赖 |
| 零拷贝 | `Span<T>` / `Memory<T>` | 减少内存分配和 GC 压力 |
| 生产者并发 | `ChannelWriter` + SpinLock | 无锁高并发写入 |
| 消费者调度 | 工作窃取调度器 (Work-Stealing) | 均衡 Partition 消费负载 |

### 5.2 性能目标（单机基准）

| 场景 | 目标值 |
|------|--------|
| 内存模式 — 吞吐量（小消息 256B） | ≥ 1,000,000 msg/s |
| 内存模式 — 吞吐量（大消息 4KB）  | ≥ 500,000 msg/s |
| 内存模式 — P99 延迟              | < 1μs |
| 持久化模式 — 吞吐量（小消息）     | ≥ 300,000 msg/s |
| 持久化模式 — 吞吐量（大消息）     | ≥ 100,000 msg/s |
| 持久化模式 — P99 延迟             | < 5ms |
| 批量生产（1000 条/批）            | 延迟 < 10ms |

### 5.3 内存优化

- 对象池化（`ObjectPool<Message<T>>`），减少 GC Alloc
- RingBuffer 使用预分配连续内存，避免内存碎片
- 支持背压（Backpressure）：生产者积压时可阻塞或丢弃

---

## 6. 可靠性设计

### 6.1 持久化保证

| 级别 | 行为 | 性能影响 |
|------|------|---------|
| `None` | 不刷盘（纯内存） | 最高 |
| `Periodic` | 定时刷盘（默认 100ms） | 中 |
| `PerWrite` | 每次写入刷盘 | 低 |

### 6.2 故障恢复

- 持久化模式：进程重启后，从 RocksDB 自动恢复所有 Topic / Partition / Offset 状态
- 消费者 Offset 持久化：重启后从上次 ACK 位置继续消费
- 损坏恢复：RocksDB 自带 WAL + CRC 校验

### 6.3 消费语义

| 语义 | 说明 | 支持 |
|------|------|------|
| At-Most-Once | 自动 ACK，收到即确认 | ✅ |
| At-Least-Once | 手动 ACK，处理成功才提交 | ✅ |
| Exactly-Once | 需要业务幂等配合 | 业务层保证 |

---

## 7. 项目结构

```
TianShu.MQ/
├── src/
│   ├── TianShu.MQ/                    # 核心类库
│   │   ├── Core/                      # 核心抽象
│   │   │   ├── Message.cs
│   │   │   ├── Topic.cs
│   │   │   ├── Partition.cs
│   │   │   ├── Offset.cs
│   │   │   └── MessageQueue.cs
│   │   ├── Producer/                  # 生产者
│   │   │   ├── IMessageProducer.cs
│   │   │   └── DefaultProducer.cs
│   │   ├── Consumer/                  # 消费者
│   │   │   ├── IMessageConsumer.cs
│   │   │   ├── ConsumerGroupManager.cs
│   │   │   ├── PartitionConsumer.cs
│   │   │   └── WorkStealingScheduler.cs
│   │   ├── Storage/                   # 存储引擎
│   │   │   ├── IStorageEngine.cs
│   │   │   ├── MemoryStorageEngine.cs # 内存模式 (Channel + RingBuffer)
│   │   │   └── StorageMode.cs
│   │   ├── Admin/                     # 管理
│   │   │   ├── IAdminClient.cs
│   │   │   └── DefaultAdminClient.cs
│   │   ├── Serialization/             # 序列化
│   │   │   └── IMessageSerializer.cs
│   │   └── TianShu.MQ.csproj
│   │
│   ├── TianShu.MQ.Persist/            # 持久化扩展
│   │   ├── PersistStorageEngine.cs    # RocksDB 存储实现
│   │   ├── IndexFileManager.cs
│   │   ├── SegmentFileManager.cs
│   │   └── TianShu.MQ.Persist.csproj
│   │
│   └── TianShu.MQ.AspNetCore/         # ASP.NET Core 集成
│       ├── ServiceCollectionExtensions.cs
│       ├── TianShuMqOptions.cs
│       └── TianShu.MQ.AspNetCore.csproj
│
├── tests/
│   ├── TianShu.MQ.Tests/
│   │   ├── Core/
│   │   ├── Storage/
│   │   ├── Producer/
│   │   └── Consumer/
│   └── TianShu.MQ.Benchmarks/         # BenchmarkDotNet 基准测试
│       └── Program.cs
│
├── docs/
│   └── PRD.md (本文件)
│
├── TianShu.MQ.slnx
└── README.md
```

---

## 8. 版本规划

### V1.0 — MVP

- [x] 内存模式（Channel + RingBuffer 实现）
- [x] Topic / Partition 模型
- [x] Producer（单条 + 批量 Publish）
- [x] Consumer（Push 模式 + ACK）
- [x] Consumer Group（分区分配）
- [x] 分区键 Hash 路由
- [x] ASP.NET Core DI 集成
- [ ] Benchmark 性能达标验证

### V1.1 — 持久化

- [ ] RocksDB 持久化存储引擎
- [ ] Offset 持久化与故障恢复
- [ ] 延迟消息
- [ ] 消息过期删除（TTL）

### V1.2 — 高级能力

- [ ] Pull 模式消费
- [ ] 死信队列（DLQ）
- [ ] 消息重试（指数退避）
- [ ] 事务支持（批量原子写入）
- [ ] 管理 API（查看 Lag、重置 Offset）

---

## 9. 竞品对比

| 特性 | TianShu.MQ | Kafka | RabbitMQ | NetMQ |
|------|-----------|-------|----------|-------|
| 部署模式 | 进程内嵌入 | 独立集群 | 独立服务 | 进程内 |
| Partition 分区 | ✅ | ✅ | ❌ | ❌ |
| 持久化 | ✅ (RocksDB) | ✅ | ✅ | ❌ |
| 吞吐量 | 极高 | 极高 | 中 | 高 |
| 延迟 | 极低(μs级) | 中(ms级) | 中(ms级) | 低 |
| 使用复杂度 | 极低(NuGet引入) | 高(需运维集群) | 中 | 中 |
| .NET 原生 | ✅ | ❌ | ❌ | ✅ |

---

## 10. 关键设计决策

| 决策 | 选项 | 选择 | 理由 |
|------|------|------|------|
| 持久化引擎 | RocksDB / SQLite / 自研 | RocksDB | 写入吞吐最高，LSM-Tree 适合 MQ |
| 内存数据结构 | Channel / BlockingCollection / RingBuffer | Channel + RingBuffer | 无锁高性能，适合生产消费模式 |
| 序列化 | JSON / Protobuf / MessagePack / MemoryPack | MemoryPack | .NET 原生二进制序列化，性能最优 |
| 分区分配 | 一致性哈希 / 简单取模 / Range | Range | 简单高效，分区数少时效果好 |
| 消费者调度 | 线程池 / Actor / 工作窃取 | 工作窃取 | 避免空闲分区阻塞，负载均衡好 |

---

> **文档版本**: v1.0  
> **更新日期**: 2026-06-24  
> **作者**: TianShu.MQ Team
