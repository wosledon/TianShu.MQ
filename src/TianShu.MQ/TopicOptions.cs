namespace TianShu.MQ.Core;

/// <summary>
/// Topic 配置选项
/// </summary>
public sealed class TopicOptions
{
    /// <summary>主题名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>分区数量</summary>
    public int Partitions { get; set; } = 1;

    /// <summary>存储模式</summary>
    public StorageMode Storage { get; set; } = StorageMode.Memory;

    /// <summary>内存模式下的最大容量（消息数量）</summary>
    public int MemoryCapacity { get; set; } = 1_000_000;

    /// <summary>刷盘策略（仅持久化模式）</summary>
    public FlushStrategy FlushStrategy { get; set; } = FlushStrategy.Periodic;
}
