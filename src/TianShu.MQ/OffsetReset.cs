namespace TianShu.MQ.Core;

/// <summary>
/// Offset 重置策略
/// </summary>
public enum OffsetReset
{
    /// <summary>重置到最早的位置（从头消费）</summary>
    Earliest,

    /// <summary>重置到最新的位置（跳过所有消息）</summary>
    Latest,

    /// <summary>重置到指定时间戳</summary>
    ToTimestamp
}
