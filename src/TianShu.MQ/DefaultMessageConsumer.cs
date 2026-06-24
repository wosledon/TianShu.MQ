using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TianShu.MQ.Core;

namespace TianShu.MQ;

/// <summary>
/// 默认消息消费者实现
/// </summary>
public sealed class DefaultMessageConsumer : IMessageConsumer
{
    private readonly MessageQueue _queue;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _subscriptions = new();
    private readonly TimeSpan _pollInterval = TimeSpan.FromMilliseconds(100);
    private readonly TimeSpan _waitTimeout = TimeSpan.FromSeconds(5);

    public DefaultMessageConsumer(MessageQueue queue)
    {
        _queue = queue;
    }

    /// <inheritdoc/>
    public async Task SubscribeAsync(
        string topic,
        string groupId,
        Func<Message, Task<ConsumeResult>> handler,
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

        // 简化版：单消费者获取所有分区
        var partitions = Enumerable.Range(0, options.Partitions).ToArray();

        // 为每个分区启动消费任务
        var tasks = partitions.Select(p => ConsumePartitionAsync(
            topic, groupId, p, handler, groupManager, engine, cts.Token));

        await Task.WhenAll(tasks);
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
                // 跳过延迟消息
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

    private async Task ConsumePartitionAsync(
        string topic,
        string groupId,
        int partition,
        Func<Message, Task<ConsumeResult>> handler,
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
                // 等待新消息
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

                // 跳过延迟消息
                if (msg.ScheduledEnqueueTime.HasValue && msg.ScheduledEnqueueTime.Value > DateTime.UtcNow)
                    continue;

                try
                {
                    var result = await handler(msg);
                    if (result == ConsumeResult.Success || result == ConsumeResult.Ignore)
                    {
                        groupManager.CommitOffset(partition, msg.Offset + 1);
                    }
                    // Failure: 不提交 offset，下次重试
                }
                catch
                {
                    // 异常时不提交，下次重试
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
        await ValueTask.CompletedTask;
    }
}
