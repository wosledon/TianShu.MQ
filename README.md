# TianShu.MQ

> 本地高性能嵌入式消息队列 NuGet 类库

TianShu.MQ 是一个运行在 .NET 应用进程内的嵌入式消息队列，提供类 Kafka 的 Topic + Partition 分区模型，支持内存和持久化两种存储模式。

## 特性

- **Kafka 分区模型** — Topic / Partition / Offset / ConsumerGroup，分区内消息严格有序
- **双存储模式** — 内存模式（Channel + RingBuffer，极致性能）和持久化模式（本地文件，重启恢复）
- **高性能** — 内存模式 P99 < 1μs，零 GC 分配
- **进程内嵌入** — 无需部署独立 Broker，NuGet 引入即用
- **Push / Pull 消费** — 支持回调推送和主动拉取两种消费模式
- **ASP.NET Core 集成** — 一行代码注册 DI

## 快速开始

### 1. 安装 NuGet 包

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
    await HandleOrder(msg);
    return ConsumeResult.Success;
});

// Pull 模式
var messages = await _consumer.PullAsync("order-events", "group1", maxBatchSize: 100);
```

## 项目结构

```
TianShu.MQ/
├── src/
│   ├── TianShu.MQ/              # 核心库（内存模式）
│   ├── TianShu.MQ.Persist/      # 持久化存储扩展
│   └── TianShu.MQ.AspNetCore/   # ASP.NET Core DI 集成
├── tests/
│   ├── TianShu.MQ.Tests/        # 单元测试
│   └── TianShu.MQ.Benchmarks/   # 性能基准测试
└── docs/
    └── PRD.md                   # 产品需求文档
```

## 构建

```bash
dotnet build
```

## 运行测试

```bash
dotnet test
```

## 许可证

MIT
