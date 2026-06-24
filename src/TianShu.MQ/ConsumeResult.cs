namespace TianShu.MQ.Core;

/// <summary>
/// 消费结果
/// </summary>
public enum ConsumeResult
{
    /// <summary>消费成功，提交 Offset</summary>
    Success,

    /// <summary>消费失败，稍后重试</summary>
    Failure,

    /// <summary>忽略此消息（不提交 Offset，也不重试）</summary>
    Ignore
}
