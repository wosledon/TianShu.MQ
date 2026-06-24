using System;
using System.Collections.Generic;
using TianShu.MQ.Core;

namespace TianShu.MQ.AspNetCore;

/// <summary>
/// TianShu MQ 配置选项
/// </summary>
public sealed class TianShuMqOptions
{
    /// <summary>
    /// 预定义的主题列表
    /// </summary>
    internal List<TopicOptions> Topics { get; } = new();

    /// <summary>
    /// 添加一个主题
    /// </summary>
    public TianShuMqOptions AddTopic(string name, Action<TopicOptions>? configure = null)
    {
        var options = new TopicOptions { Name = name };
        configure?.Invoke(options);
        Topics.Add(options);
        return this;
    }
}
