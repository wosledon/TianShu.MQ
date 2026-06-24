namespace TianShu.MQ.Core;

/// <summary>
/// 分区统计信息
/// </summary>
public sealed class PartitionStats
{
    public int PartitionId { get; init; }
    public long StartOffset { get; init; }
    public long EndOffset { get; init; }
    public long MessageCount { get; init; }
}

/// <summary>
/// 主题统计信息
/// </summary>
public sealed class TopicStats
{
    public string Topic { get; init; } = string.Empty;
    public int PartitionCount { get; init; }
    public long TotalMessages { get; init; }
    public PartitionStats[] Partitions { get; init; } = Array.Empty<PartitionStats>();
}

/// <summary>
/// 消费者组进度
/// </summary>
public sealed class ConsumerGroupProgress
{
    public string Topic { get; init; } = string.Empty;
    public string GroupId { get; init; } = string.Empty;
    public PartitionProgress[] Partitions { get; init; } = Array.Empty<PartitionProgress>();
}

/// <summary>
/// 分区消费进度
/// </summary>
public sealed class PartitionProgress
{
    public int PartitionId { get; init; }
    public long CommittedOffset { get; init; }
    public long LatestOffset { get; init; }
    public long Lag => LatestOffset - CommittedOffset;
}
