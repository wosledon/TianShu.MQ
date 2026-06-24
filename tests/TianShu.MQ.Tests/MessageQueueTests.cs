using System;
using System.Text;
using System.Threading.Tasks;
using TianShu.MQ.Core;
using Xunit;

namespace TianShu.MQ.Tests;

public class MessageQueueTests
{
    [Fact]
    public async Task CreateTopic_ShouldSucceed()
    {
        await using var mq = new MessageQueue();
        var options = new TopicOptions { Name = "test-topic", Partitions = 3, Storage = StorageMode.Memory };

        await mq.CreateTopicAsync(options);

        var topics = mq.ListTopics();
        Assert.Contains("test-topic", topics);
    }

    [Fact]
    public async Task CreateTopic_Duplicate_ShouldThrow()
    {
        await using var mq = new MessageQueue();
        var options = new TopicOptions { Name = "test-topic", Partitions = 1 };

        await mq.CreateTopicAsync(options);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mq.CreateTopicAsync(options));
    }

    [Fact]
    public async Task PublishAndConsume_ShouldWork()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "orders", Partitions = 2, Storage = StorageMode.Memory });

        var producer = new DefaultMessageProducer(mq);
        var consumer = new DefaultMessageConsumer(mq);

        var msg = new Message
        {
            Body = Encoding.UTF8.GetBytes("hello"),
            PartitionKey = "key1"
        };

        await producer.PublishAsync("orders", msg);

        var pulled = await consumer.PullAsync("orders", "group1", 10);
        Assert.Single(pulled);
        Assert.Equal("hello", Encoding.UTF8.GetString(pulled[0].Body));
    }

    [Fact]
    public async Task BatchPublish_ShouldWork()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "batch-topic", Partitions = 3, Storage = StorageMode.Memory });

        var producer = new DefaultMessageProducer(mq);
        var consumer = new DefaultMessageConsumer(mq);

        var messages = new Message[10];
        for (int i = 0; i < 10; i++)
        {
            messages[i] = new Message
            {
                Body = Encoding.UTF8.GetBytes($"msg-{i}"),
                PartitionKey = $"key{i % 3}"
            };
        }

        await producer.PublishBatchAsync("batch-topic", messages);

        var pulled = await consumer.PullAsync("batch-topic", "group1", 100);
        Assert.Equal(10, pulled.Length);
    }

    [Fact]
    public async Task PartitionKey_Routing_ShouldBeDeterministic()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "routing", Partitions = 3, Storage = StorageMode.Memory });

        var producer = new DefaultMessageProducer(mq);

        // 同一个 key 应该路由到同一个分区
        for (int i = 0; i < 5; i++)
        {
            await producer.PublishAsync("routing", new Message
            {
                Body = Encoding.UTF8.GetBytes($"msg-{i}"),
                PartitionKey = "same-key"
            });
        }

        // 检查统计：所有消息应该在同一个分区
        var stats = await mq.GetTopicStatsAsync("routing");
        Assert.Equal(3, stats.PartitionCount);
        Assert.Equal(5, stats.TotalMessages);

        // 有消息的分区应该只有 1 个
        var activePartitions = 0;
        foreach (var p in stats.Partitions)
        {
            if (p.MessageCount > 0) activePartitions++;
        }
        Assert.Equal(1, activePartitions);
    }

    [Fact]
    public async Task ConsumerGroup_CommitOffset_ShouldAdvance()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "cg-test", Partitions = 1, Storage = StorageMode.Memory });

        var producer = new DefaultMessageProducer(mq);
        var consumer = new DefaultMessageConsumer(mq);

        for (int i = 0; i < 5; i++)
        {
            await producer.PublishAsync("cg-test", new Message
            {
                Body = Encoding.UTF8.GetBytes($"msg-{i}"),
                PartitionKey = "k"
            });
        }

        // 第一次拉取 3 条
        var batch1 = await consumer.PullAsync("cg-test", "group1", 3);
        Assert.Equal(3, batch1.Length);

        // 第二次拉取应获取剩余 2 条
        var batch2 = await consumer.PullAsync("cg-test", "group1", 10);
        Assert.Equal(2, batch2.Length);
    }

    [Fact]
    public async Task GetTopicStats_ShouldReturnCorrectInfo()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "stats-topic", Partitions = 2, Storage = StorageMode.Memory });

        var producer = new DefaultMessageProducer(mq);

        for (int i = 0; i < 10; i++)
        {
            await producer.PublishAsync("stats-topic", new Message
            {
                Body = Encoding.UTF8.GetBytes($"msg-{i}"),
                PartitionKey = $"key{i}"
            });
        }

        var stats = await mq.GetTopicStatsAsync("stats-topic");
        Assert.Equal("stats-topic", stats.Topic);
        Assert.Equal(2, stats.PartitionCount);
        Assert.Equal(10, stats.TotalMessages);
    }

    [Fact]
    public async Task AdminClient_ResetOffset_ShouldWork()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "reset-test", Partitions = 1, Storage = StorageMode.Memory });

        var producer = new DefaultMessageProducer(mq);
        var consumer = new DefaultMessageConsumer(mq);
        var admin = new DefaultAdminClient(mq);

        for (int i = 0; i < 5; i++)
        {
            await producer.PublishAsync("reset-test", new Message
            {
                Body = Encoding.UTF8.GetBytes($"msg-{i}"),
                PartitionKey = "k"
            });
        }

        // 消费全部
        var all = await consumer.PullAsync("reset-test", "group1", 100);
        Assert.Equal(5, all.Length);

        // 重置到最早
        await admin.ResetOffsetAsync("reset-test", "group1", OffsetReset.Earliest);

        // 重新消费
        var reConsumed = await consumer.PullAsync("reset-test", "group1", 100);
        Assert.Equal(5, reConsumed.Length);
    }

    [Fact]
    public async Task AdminClient_ListTopics_ShouldReturnAll()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "topic-a", Partitions = 1 });
        await mq.CreateTopicAsync(new TopicOptions { Name = "topic-b", Partitions = 2 });
        await mq.CreateTopicAsync(new TopicOptions { Name = "topic-c", Partitions = 3 });

        var admin = new DefaultAdminClient(mq);
        var topics = admin.ListTopics();

        Assert.Equal(3, topics.Length);
        Assert.Contains("topic-a", topics);
        Assert.Contains("topic-b", topics);
        Assert.Contains("topic-c", topics);
    }

    [Fact]
    public async Task DeleteTopic_ShouldRemoveTopic()
    {
        await using var mq = new MessageQueue();
        await mq.CreateTopicAsync(new TopicOptions { Name = "to-delete", Partitions = 1 });

        var topics = mq.ListTopics();
        Assert.Contains("to-delete", topics);

        await mq.DeleteTopicAsync("to-delete");

        topics = mq.ListTopics();
        Assert.DoesNotContain("to-delete", topics);
    }
}
