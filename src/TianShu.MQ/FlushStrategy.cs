namespace TianShu.MQ.Core;

/// <summary>
/// 刷盘策略
/// </summary>
public enum FlushStrategy
{
    /// <summary>仅内存操作，不刷盘（性能最高）</summary>
    None,

    /// <summary>定时刷盘（默认每 100ms）</summary>
    Periodic,

    /// <summary>每次写入立即刷盘（最可靠，性能最低）</summary>
    PerWrite
}
