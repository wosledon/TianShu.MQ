using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TianShu.MQ.Core;
using TianShu.MQ.Serialization;

namespace TianShu.MQ;

/// <summary>
/// 默认消息生产者实现
/// </summary>
public sealed class DefaultMessageProducer : IMessageProducer
{
    private readonly MessageQueue _queue;
    private readonly IMessageSerializer _serializer;

    public DefaultMessageProducer(MessageQueue queue) : this(queue, new DefaultMessageSerializer())
    {
    }

    public DefaultMessageProducer(MessageQueue queue, IMessageSerializer serializer)
    {
        _queue = queue;
        _serializer = serializer;
    }

    /// <inheritdoc/>
    public async Task PublishAsync(string topic, Message message, CancellationToken cancellationToken = default)
    {
        var options = _queue.GetTopicOptions(topic)
            ?? throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var partition = ResolvePartition(message.PartitionKey, options.Partitions);
        var engine = _queue.GetStorageEngine(topic);

        await engine.AppendAsync(topic, partition, message, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task PublishAsync<T>(string topic, string? partitionKey, T body, CancellationToken cancellationToken = default)
    {
        var msg = new Message
        {
            Body = _serializer.Serialize(body),
            PartitionKey = partitionKey ?? string.Empty
        };
        await PublishAsync(topic, msg, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task PublishAsync<T>(string topic, string? partitionKey, T body, DateTime scheduledTime, CancellationToken cancellationToken = default)
    {
        var msg = new Message
        {
            Body = _serializer.Serialize(body),
            PartitionKey = partitionKey ?? string.Empty,
            ScheduledEnqueueTime = scheduledTime
        };
        await PublishAsync(topic, msg, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task PublishBatchAsync(string topic, Message[] messages, CancellationToken cancellationToken = default)
    {
        var options = _queue.GetTopicOptions(topic)
            ?? throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        // 按分区分组
        var grouped = new Dictionary<int, List<Message>>();
        foreach (var msg in messages)
        {
            var partition = ResolvePartition(msg.PartitionKey, options.Partitions);
            if (!grouped.TryGetValue(partition, out var list))
            {
                list = new List<Message>();
                grouped[partition] = list;
            }
            list.Add(msg);
        }

        var engine = _queue.GetStorageEngine(topic);

        // 按分区批量写入
        var tasks = grouped.Select(kvp =>
            engine.AppendBatchAsync(topic, kvp.Key, kvp.Value.ToArray(), cancellationToken));

        await Task.WhenAll(tasks);
    }

    /// <inheritdoc/>
    public long[] GetTopicOffsets(string topic)
    {
        var options = _queue.GetTopicOptions(topic)
            ?? throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var engine = _queue.GetStorageEngine(topic);
        var offsets = new long[options.Partitions];

        for (int i = 0; i < options.Partitions; i++)
        {
            offsets[i] = engine.GetLatestOffset(topic, i);
        }

        return offsets;
    }

    /// <summary>
    /// 根据 partition key 计算分区（Hash 取模）
    /// </summary>
    private static int ResolvePartition(string partitionKey, int partitionCount)
    {
        if (string.IsNullOrEmpty(partitionKey))
        {
            // 无 key，轮询分配
            return Random.Shared.Next(partitionCount);
        }

        // 使用 MurmurHash3 风格哈希，保证均匀分布
        var hash = GetStableHashCode(partitionKey);
        return (hash & 0x7FFFFFFF) % partitionCount;
    }

    /// <summary>
    /// 稳定的字符串哈希函数
    /// </summary>
    private static int GetStableHashCode(string str)
    {
        unchecked
        {
            int hash1 = 5381;
            int hash2 = hash1;

            for (int i = 0; i < str.Length; i += 2)
            {
                hash1 = ((hash1 << 5) + hash1) ^ str[i];
                if (i + 1 < str.Length)
                    hash2 = ((hash2 << 5) + hash2) ^ str[i + 1];
            }

            return hash1 + (hash2 * 1566083941);
        }
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
