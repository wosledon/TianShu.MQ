# TianShu.MQ

> A high-performance embedded message queue NuGet library for .NET

---

## Overview

TianShu.MQ is an in-process embedded message queue for .NET applications, featuring a Kafka-like Topic + Partition model with both in-memory and persistent storage modes. No standalone broker deployment is required — just add the NuGet package and integrate. Ideal for microservices and monolithic applications needing local messaging.

## Features

- **Kafka Partition Model** — Topic / Partition / Offset / ConsumerGroup with strict ordering within a partition
- **Dual Storage Modes** — In-memory (RingBuffer + Channel) and persistent (local file, crash recovery)
- **High Performance** — P99 < 1μs in memory mode, zero GC allocation optimized
- **Fully Embedded** — Runs inside your process, no broker deployment needed
- **Push & Pull** — Supports both callback-based push and active pull consumption
- **ASP.NET Core Integration** — Register with a single line of code via DI
- **Batch Operation** — Supports batch production and consumption for higher throughput

## Quick Start

### 1. Install

```bash
dotnet add package TianShu.MQ
dotnet add package TianShu.MQ.AspNetCore
```

### 2. Register Services

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

### 3. Produce Messages

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
            PartitionKey = dto.OrderId  // same key → same partition, ordered
        });
        return Ok();
    }
}
```

### 4. Consume Messages

```csharp
// Push mode
await _consumer.SubscribeAsync("order-events", "payment-group", async msg =>
{
    await HandleOrder(msg.Body);
    return ConsumeResult.Success;
});

// Pull mode
var messages = await _consumer.PullAsync("order-events", "group1", maxBatchSize: 100);
```

## API Overview

| Interface | Implementation | Description |
|---|---|---|
| `IMessageProducer` | `DefaultMessageProducer` | Produce messages, single/batch, partition key routing |
| `IMessageConsumer` | `DefaultMessageConsumer` | Consume messages, Push & Pull modes |
| `IAdminClient` | `DefaultAdminClient` | Manage topics, view stats, reset offsets |
| `IStorageEngine` | `MemoryStorageEngine` / `PersistStorageEngine` | Storage engine, extensible |

## Project Structure

```
TianShu.MQ/
├── src/
│   ├── TianShu.MQ/                   # Core library
│   │   ├── Core/                     # Message, TopicOptions, enums
│   │   ├── Storage/                  # Storage interface & memory impl
│   │   ├── DefaultMessageProducer.cs
│   │   ├── DefaultMessageConsumer.cs
│   │   ├── DefaultAdminClient.cs
│   │   ├── ConsumerGroupManager.cs
│   │   └── MessageQueue.cs           # Core engine
│   ├── TianShu.MQ.Persist/           # Persist storage extension
│   │   └── PersistStorageEngine.cs
│   └── TianShu.MQ.AspNetCore/        # ASP.NET Core DI integration
│       ├── ServiceCollectionExtensions.cs
│       └── TianShuMqOptions.cs
├── tests/
│   ├── TianShu.MQ.Tests/             # Unit tests (14 test cases)
│   └── TianShu.MQ.Benchmarks/       # Benchmarks
├── docs/
│   └── PRD.md                        # Product requirements document
├── LICENSE
├── README.md                         # 中文文档
└── README.en.md                      # English document
```

## Build & Test

```bash
# Build
dotnet build

# Run tests
dotnet test
```

## Roadmap

| Version | Features |
|---|---|
| 1.0 (current) | In-memory mode, Topic/Partition, Producer/Consumer, Push/Pull, ASP.NET Core integration |
| 1.1 | Persistent storage, offset recovery, delayed messages, TTL |
| 1.2 | Dead letter queue, message retry (exponential backoff), admin API, transactions |

---

## License

[MIT](LICENSE) © 2026 TianShu
