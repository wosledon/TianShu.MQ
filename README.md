# TianShu.MQ

> 本地高性能嵌入式消息队列 NuGet 类库

---

## 概述

TianShu.MQ 是一个运行在 .NET 应用进程内的嵌入式消息队列，提供类 Kafka 的 Topic + Partition 分区模型，支持内存和持久化两种存储模式。无需部署独立 Broker，NuGet 引入即用，是微服务和单体应用的理想本地消息中间件。

## 特性

- **Kafka 分区模型** — Topic / Partition / Offset / ConsumerGroup，分区内消息严格有序
- **双存储模式** — 内存模式（RingBuffer + Channel）和持久化模式（本地文件，重启恢复）
- **高性能** — 内存模式 P99 < 1μs，零 GC 分配优化
- **进程内嵌入** — 无需部署独立 Broker
- **Push / Pull 消费** — 支持回调推送和主动拉取两种模式
- **ASP.NET Core 集成** — 一行代码注册 DI，开箱即用
- **批量操作** — 支持批量生产和消费，吞吐量更高

## 快速开始

### 1. 安装

```bash
dotnet add package TianShu.MQ
dotnet add package TianShu.MQ.AspNetCore
```

### 2. 注册服务

```csharp
// Program.cs
builder.Services.AddTianShuMQ(mq =>
{
    mq.AddTopic("order-events", options =>
    {
        options.Partitions = 3;
        options.Storage = StorageMode.Memory;
    });

    mq.AddTopic("analytics", options =>
    {
        options.Partitions = 4;
        options.Storage = StorageMode.Persist;
    });
});
```

### 3. 生产消息

```csharp
public class OrderController : ControllerBase
{
    private readonly IMessageProducer _producer;

    public OrderController(IMessageProducer producer) => _producer = producer;

    [HttpPost]
    public async Task<IActionResult> CreateOrder(OrderDto dto)
    {
        await _producer.PublishAsync("order-events", new Message
        {
            Body = JsonSerializer.SerializeToUtf8Bytes(dto),
            PartitionKey = dto.OrderId  // 同 key 保证同分区顺序
        });
        return Ok();
    }
}
```

### 4. 消费消息

```csharp
// Push 模式
await _consumer.SubscribeAsync("order-events", "payment-group", async msg =>
{
    await HandleOrder(msg.Body);
    return ConsumeResult.Success;
});

// Pull 模式
var messages = await _consumer.PullAsync("order-events", "group1", maxBatchSize: 100);
```

## API 概览

| 接口 | 实现 | 说明 |
|---|---|---|
| `IMessageProducer` | `DefaultMessageProducer` | 生产消息，支持单条/批量，分区键路由 |
| `IMessageConsumer` | `DefaultMessageConsumer` | 消费消息，支持 Push/Pull 模式 |
| `IAdminClient` | `DefaultAdminClient` | 管理 Topic、查看统计、重置 Offset |
| `IStorageEngine` | `MemoryStorageEngine` / `PersistStorageEngine` | 存储引擎，可扩展 |

## 项目结构

```
TianShu.MQ/
├── src/
│   ├── TianShu.MQ/                   # 核心库
│   │   ├── Core/                     # 消息、Topic 选项、枚举
│   │   ├── Storage/                  # 存储引擎接口和内存实现
│   │   ├── DefaultMessageProducer.cs
│   │   ├── DefaultMessageConsumer.cs
│   │   ├── DefaultAdminClient.cs
│   │   ├── ConsumerGroupManager.cs
│   │   └── MessageQueue.cs           # 核心引擎
│   ├── TianShu.MQ.Persist/           # 持久化存储扩展
│   │   └── PersistStorageEngine.cs
│   └── TianShu.MQ.AspNetCore/        # ASP.NET Core DI 集成
│       ├── ServiceCollectionExtensions.cs
│       └── TianShuMqOptions.cs
├── tests/
│   ├── TianShu.MQ.Tests/             # 单元测试（14 个用例）
│   └── TianShu.MQ.Benchmarks/       # 性能基准测试
├── docs/
│   └── PRD.md                        # 产品需求文档
├── LICENSE
└── README.md
```

## 构建与测试

```bash
# 构建
dotnet build

# 运行测试
dotnet test
```

## 路线图

| 版本 | 功能 |
|---|---|
| 1.0 (当前) | 内存模式、Topic/Partition、Producer/Consumer、Push/Pull、ASP.NET Core 集成 |
| 1.1 | 持久化存储、Offset 故障恢复、延迟消息、TTL 过期 |


## 许可证

[MIT](LICENSE) © 2026 TianShu

## 许可证

MIT
