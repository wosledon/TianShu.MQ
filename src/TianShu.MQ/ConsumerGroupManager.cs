using System.Collections.Concurrent;
using System.Threading;

namespace TianShu.MQ;

/// <summary>
/// 消费者组管理器 - 跟踪每个分区的消费进度
/// </summary>
public sealed class ConsumerGroupManager
{
    public string Topic { get; }
    public string GroupId { get; }

    private readonly ConcurrentDictionary<int, long> _committedOffsets = new();
    private readonly ConcurrentDictionary<int, SemaphoreSlim> _offsetLocks = new();

    public ConsumerGroupManager(string topic, string groupId)
    {
        Topic = topic;
        GroupId = groupId;
    }

    /// <summary>
    /// 获取已提交的 offset
    /// </summary>
    public long GetCommittedOffset(int partition)
    {
        return _committedOffsets.GetValueOrDefault(partition, 0);
    }

    /// <summary>
    /// 提交 offset
    /// </summary>
    public void CommitOffset(int partition, long offset)
    {
        _committedOffsets.AddOrUpdate(partition, offset, (_, existing) =>
            offset > existing ? offset : existing);
    }

    /// <summary>
    /// 获取并分配分区给消费者
    /// </summary>
    public int[] AssignPartitions(int totalPartitions, int consumerIndex, int totalConsumers)
    {
        if (totalConsumers <= 0) return System.Array.Empty<int>();

        var partitions = new System.Collections.Generic.List<int>();
        for (int i = consumerIndex; i < totalPartitions; i += totalConsumers)
        {
            partitions.Add(i);
        }
        return partitions.ToArray();
    }

    /// <summary>
    /// 重置所有分区的 offset（包括尚未提交的分区）
    /// </summary>
    public void ResetAllOffsets(long offset, int totalPartitions)
    {
        for (int i = 0; i < totalPartitions; i++)
        {
            _committedOffsets[i] = offset;
        }
    }

    /// <summary>
    /// 重置指定分区的 offset
    /// </summary>
    public void ResetOffset(int partition, long offset)
    {
        _committedOffsets[partition] = offset;
    }
}
