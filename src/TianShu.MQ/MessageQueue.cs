using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TianShu.MQ.Core;
using TianShu.MQ.Storage;

namespace TianShu.MQ;

/// <summary>
/// 消息队列核心引擎
/// </summary>
public sealed class MessageQueue : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, TopicOptions> _topicOptions = new();
    private readonly ConcurrentDictionary<string, IStorageEngine> _storageEngines = new();
    private readonly ConcurrentDictionary<string, ConsumerGroupManager> _consumerGroups = new();

    private readonly IStorageEngine _defaultMemoryEngine;

    public MessageQueue()
    {
        _defaultMemoryEngine = new MemoryStorageEngine();
    }

    /// <summary>
    /// 创建主题
    /// </summary>
    public async Task CreateTopicAsync(TopicOptions options, CancellationToken cancellationToken = default)
    {
        if (!_topicOptions.TryAdd(options.Name, options))
            throw new InvalidOperationException($"Topic '{options.Name}' already exists.");

        IStorageEngine engine = GetOrCreateStorageEngine(options.Name, options.Storage);
        await engine.InitializeAsync(options.Name, options.Partitions, cancellationToken);
    }

    /// <summary>
    /// 删除主题
    /// </summary>
    public async Task DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        _topicOptions.TryRemove(topic, out _);
        if (_storageEngines.TryGetValue(topic, out var engine))
        {
            await engine.DeleteAsync(topic, cancellationToken);
            _storageEngines.TryRemove(topic, out _);
        }
    }

    /// <summary>
    /// 获取存储引擎
    /// </summary>
    public IStorageEngine GetStorageEngine(string topic)
    {
        if (_storageEngines.TryGetValue(topic, out var engine))
            return engine;

        // 回退到默认内存引擎
        return _defaultMemoryEngine;
    }

    /// <summary>
    /// 获取消费者组管理器
    /// </summary>
    public ConsumerGroupManager GetConsumerGroupManager(string topic, string groupId)
    {
        var key = $"{topic}:{groupId}";
        return _consumerGroups.GetOrAdd(key, _ => new ConsumerGroupManager(topic, groupId));
    }

    /// <summary>
    /// 获取主题选项
    /// </summary>
    public TopicOptions? GetTopicOptions(string topic)
    {
        return _topicOptions.TryGetValue(topic, out var options) ? options : null;
    }

    /// <summary>
    /// 列出所有主题
    /// </summary>
    public string[] ListTopics()
    {
        return _topicOptions.Keys.ToArray();
    }

    /// <summary>
    /// 获取主题统计信息
    /// </summary>
    public Task<TopicStats> GetTopicStatsAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (!_topicOptions.TryGetValue(topic, out var options))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var engine = GetStorageEngine(topic);
        var partitions = new PartitionStats[options.Partitions];
        long totalMessages = 0;

        for (int i = 0; i < options.Partitions; i++)
        {
            var latest = engine.GetLatestOffset(topic, i);
            var earliest = engine.GetEarliestOffset(topic, i);
            var count = latest - earliest;
            totalMessages += count;

            partitions[i] = new PartitionStats
            {
                PartitionId = i,
                StartOffset = earliest,
                EndOffset = latest,
                MessageCount = count
            };
        }

        return Task.FromResult(new TopicStats
        {
            Topic = topic,
            PartitionCount = options.Partitions,
            TotalMessages = totalMessages,
            Partitions = partitions
        });
    }

    /// <summary>
    /// 获取消费者组进度
    /// </summary>
    public Task<ConsumerGroupProgress> GetConsumerGroupProgressAsync(string topic, string groupId, CancellationToken cancellationToken = default)
    {
        if (!_topicOptions.TryGetValue(topic, out var options))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var engine = GetStorageEngine(topic);
        var groupManager = GetConsumerGroupManager(topic, groupId);
        var partitions = new PartitionProgress[options.Partitions];

        for (int i = 0; i < options.Partitions; i++)
        {
            partitions[i] = new PartitionProgress
            {
                PartitionId = i,
                CommittedOffset = groupManager.GetCommittedOffset(i),
                LatestOffset = engine.GetLatestOffset(topic, i)
            };
        }

        return Task.FromResult(new ConsumerGroupProgress
        {
            Topic = topic,
            GroupId = groupId,
            Partitions = partitions
        });
    }

    private IStorageEngine GetOrCreateStorageEngine(string topic, StorageMode mode)
    {
        return _storageEngines.GetOrAdd(topic, _ =>
        {
            return mode switch
            {
                StorageMode.Memory => _defaultMemoryEngine,
                StorageMode.Persist => throw new NotSupportedException(
                    "Persist storage requires TianShu.MQ.Persist package."),
                _ => _defaultMemoryEngine
            };
        });
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var engine in _storageEngines.Values)
        {
            await engine.DisposeAsync();
        }
        await _defaultMemoryEngine.DisposeAsync();
        _storageEngines.Clear();
        _topicOptions.Clear();
        _consumerGroups.Clear();
    }
}
