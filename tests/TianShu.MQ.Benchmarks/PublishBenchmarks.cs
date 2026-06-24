using System;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using TianShu.MQ.Core;

namespace TianShu.MQ.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80, iterationCount: 3)]
public class PublishBenchmarks
{
    private MessageQueue _queue = null!;
    private DefaultMessageProducer _producer = null!;

    [GlobalSetup]
    public void Setup()
    {
        _queue = new MessageQueue();
        _queue.CreateTopicAsync(new TopicOptions
        {
            Name = "bench-topic",
            Partitions = 4,
            Storage = StorageMode.Memory
        }).GetAwaiter().GetResult();

        _producer = new DefaultMessageProducer(_queue);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _queue.DisposeAsync();
    }

    [Benchmark(Description = "Publish single message (256B)")]
    public async Task PublishSingle_SmallMessage()
    {
        var msg = new Message
        {
            Body = new byte[256],
            PartitionKey = "bench-key"
        };
        await _producer.PublishAsync("bench-topic", msg);
    }

    [Benchmark(Description = "Publish single message (4KB)")]
    public async Task PublishSingle_LargeMessage()
    {
        var msg = new Message
        {
            Body = new byte[4096],
            PartitionKey = "bench-key"
        };
        await _producer.PublishAsync("bench-topic", msg);
    }

    [Benchmark(Description = "Publish batch (1000 messages)")]
    public async Task PublishBatch_1000Messages()
    {
        var messages = new Message[1000];
        for (int i = 0; i < 1000; i++)
        {
            messages[i] = new Message
            {
                Body = new byte[256],
                PartitionKey = $"key{i % 4}"
            };
        }
        await _producer.PublishBatchAsync("bench-topic", messages);
    }

    [Benchmark(Description = "Publish no partition key")]
    public async Task PublishSingle_NoPartitionKey()
    {
        var msg = new Message
        {
            Body = new byte[256]
        };
        await _producer.PublishAsync("bench-topic", msg);
    }
}
