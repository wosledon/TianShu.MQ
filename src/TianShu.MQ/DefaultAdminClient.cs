using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TianShu.MQ.Core;

namespace TianShu.MQ;

/// <summary>
/// 默认管理客户端实现
/// </summary>
public sealed class DefaultAdminClient : IAdminClient
{
    private readonly MessageQueue _queue;

    public DefaultAdminClient(MessageQueue queue)
    {
        _queue = queue;
    }

    /// <inheritdoc/>
    public Task CreateTopicAsync(TopicOptions options, CancellationToken cancellationToken = default)
    {
        return _queue.CreateTopicAsync(options, cancellationToken);
    }

    /// <inheritdoc/>
    public Task DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        return _queue.DeleteTopicAsync(topic, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<TopicStats> GetTopicStatsAsync(string topic, CancellationToken cancellationToken = default)
    {
        return _queue.GetTopicStatsAsync(topic, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<ConsumerGroupProgress> GetConsumerGroupProgressAsync(string topic, string groupId, CancellationToken cancellationToken = default)
    {
        return _queue.GetConsumerGroupProgressAsync(topic, groupId, cancellationToken);
    }

    /// <inheritdoc/>
    public Task ResetOffsetAsync(string topic, string groupId, OffsetReset strategy, DateTime? timestamp = null, CancellationToken cancellationToken = default)
    {
        var groupManager = _queue.GetConsumerGroupManager(topic, groupId);
        var engine = _queue.GetStorageEngine(topic);
        var options = _queue.GetTopicOptions(topic)
            ?? throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        for (int p = 0; p < options.Partitions; p++)
        {
            long newOffset = strategy switch
            {
                OffsetReset.Earliest => engine.GetEarliestOffset(topic, p),
                OffsetReset.Latest => engine.GetLatestOffset(topic, p),
                OffsetReset.ToTimestamp => FindOffsetByTimestamp(engine, topic, p, timestamp ?? DateTime.UtcNow),
                _ => throw new ArgumentOutOfRangeException(nameof(strategy))
            };

            groupManager.ResetOffset(p, newOffset);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public string[] ListTopics()
    {
        return _queue.ListTopics();
    }

    private static long FindOffsetByTimestamp(Storage.IStorageEngine engine, string topic, int partition, DateTime timestamp)
    {
        // 二分查找最近的 offset
        var earliest = engine.GetEarliestOffset(topic, partition);
        var latest = engine.GetLatestOffset(topic, partition);

        if (earliest >= latest) return earliest;

        // 线性扫描（对于大量消息可优化为二分）
        var batchSize = 100;
        for (long offset = earliest; offset < latest; offset += batchSize)
        {
            var messages = engine.ReadAsync(topic, partition, offset, batchSize, CancellationToken.None)
                .GetAwaiter().GetResult();

            foreach (var msg in messages)
            {
                if (msg.Timestamp >= timestamp)
                    return msg.Offset;
            }
        }

        return latest;
    }
}
