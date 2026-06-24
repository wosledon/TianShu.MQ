using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TianShu.MQ.Core;

namespace TianShu.MQ;

/// <summary>
/// ACK 实现
/// </summary>
internal sealed class Ack : IAck
{
    private readonly ConsumerGroupManager _groupManager;
    private readonly int _partition;
    private readonly long _offset;
    private bool _committed;

    public Ack(ConsumerGroupManager groupManager, int partition, long offset)
    {
        _groupManager = groupManager;
        _partition = partition;
        _offset = offset;
    }

    public Task CommitAsync()
    {
        if (!_committed)
        {
            _groupManager.CommitOffset(_partition, _offset + 1);
            _committed = true;
        }
        return Task.CompletedTask;
    }

    public Task RejectAsync()
    {
        _committed = true; // 标记已处理，但不提交 offset
        return Task.CompletedTask;
    }
}

/// <summary>
/// 默认消息消费者实现
/// </summary>
public sealed class DefaultMessageConsumer : IMessageConsumer
{
    private readonly MessageQueue _queue;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _subscriptions = new();
    private readonly TimeSpan _waitTimeout = TimeSpan.FromSeconds(5);
    private readonly int _consumerIndex;
    private static int _globalConsumerCount;

    public DefaultMessageConsumer(MessageQueue queue)
    {
        _queue = queue;
        _consumerIndex = Interlocked.Increment(ref _globalConsumerCount) - 1;
    }

    /// <inheritdoc/>
    public async Task SubscribeAsync(
        string topic,
        string groupId,
        Func<Message, Task<ConsumeResult>> handler,
        CancellationToken cancellationToken = default)
    {
        await SubscribeCoreAsync(topic, groupId,
            async (msg, partition, groupManager, engine, ct) =>
            {
                var result = await handler(msg);
                if (result == ConsumeResult.Success || result == ConsumeResult.Ignore)
                {
                    groupManager.CommitOffset(partition, msg.Offset + 1);
                }
            }, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SubscribeAsync(
        string topic,
        string groupId,
        Func<Message, IAck, Task> handler,
        CancellationToken cancellationToken = default)
    {
        await SubscribeCoreAsync(topic, groupId,
            async (msg, partition, groupManager, engine, ct) =>
            {
                var ack = new Ack(groupManager, partition, msg.Offset);
                await handler(msg, ack);
            }, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<Message[]> PullAsync(
        string topic,
        string groupId,
        int maxBatchSize = 100,
        CancellationToken cancellationToken = default)
    {
        var options = _queue.GetTopicOptions(topic)
            ?? throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var groupManager = _queue.GetConsumerGroupManager(topic, groupId);
        var engine = _queue.GetStorageEngine(topic);
        var result = new List<Message>();

        for (int p = 0; p < options.Partitions && result.Count < maxBatchSize; p++)
        {
            var offset = groupManager.GetCommittedOffset(p);
            var remaining = maxBatchSize - result.Count;
            var messages = await engine.ReadAsync(topic, p, offset, remaining, cancellationToken);

            foreach (var msg in messages)
            {
                // 跳过延迟消息（未到投递时间）
                if (msg.ScheduledEnqueueTime.HasValue && msg.ScheduledEnqueueTime.Value > DateTime.UtcNow)
                    continue;

                result.Add(msg);
            }

            if (messages.Length > 0)
            {
                groupManager.CommitOffset(p, messages[^1].Offset + 1);
            }
        }

        return result.ToArray();
    }

    /// <inheritdoc/>
    public Task UnsubscribeAsync(string topic, string groupId, CancellationToken cancellationToken = default)
    {
        var key = $"{topic}:{groupId}";
        if (_subscriptions.TryRemove(key, out var cts))
        {
            cts.Cancel();
            cts.Dispose();
        }
        return Task.CompletedTask;
    }

    private async Task SubscribeCoreAsync(
        string topic,
        string groupId,
        Func<Message, int, ConsumerGroupManager, Storage.IStorageEngine, CancellationToken, Task> handleMessage,
        CancellationToken cancellationToken = default)
    {
        var key = $"{topic}:{groupId}";
        if (_subscriptions.ContainsKey(key))
            throw new InvalidOperationException($"Already subscribed to '{topic}' with group '{groupId}'.");

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _subscriptions[key] = cts;

        var options = _queue.GetTopicOptions(topic)
            ?? throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var groupManager = _queue.GetConsumerGroupManager(topic, groupId);
        var engine = _queue.GetStorageEngine(topic);

        // 使用 ConsumerGroupManager 分配分区
        var partitions = groupManager.AssignPartitions(options.Partitions, _consumerIndex, _globalConsumerCount);

        // 为每个分配的分区启动消费任务
        var tasks = partitions.Select(p => ConsumePartitionAsync(
            topic, p, handleMessage, groupManager, engine, cts.Token));

        await Task.WhenAll(tasks);
    }

    private async Task ConsumePartitionAsync(
        string topic,
        int partition,
        Func<Message, int, ConsumerGroupManager, Storage.IStorageEngine, CancellationToken, Task> handleMessage,
        ConsumerGroupManager groupManager,
        Storage.IStorageEngine engine,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var offset = groupManager.GetCommittedOffset(partition);
            var latest = engine.GetLatestOffset(topic, partition);

            if (offset >= latest)
            {
                try
                {
                    await engine.WaitForMessagesAsync(topic, partition, offset, _waitTimeout, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                continue;
            }

            // 读取并处理消息
            var messages = await engine.ReadAsync(topic, partition, offset, 100, cancellationToken);
            foreach (var msg in messages)
            {
                if (cancellationToken.IsCancellationRequested) break;

                // 跳过延迟消息（未到投递时间）
                if (msg.ScheduledEnqueueTime.HasValue && msg.ScheduledEnqueueTime.Value > DateTime.UtcNow)
                    continue;

                try
                {
                    await handleMessage(msg, partition, groupManager, engine, cancellationToken);
                }
                catch
                {
                    // 异常时不提交 offset，下次重试
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var kvp in _subscriptions)
        {
            kvp.Value.Cancel();
            kvp.Value.Dispose();
        }
        _subscriptions.Clear();
    }
}
