using System;
using Microsoft.Extensions.DependencyInjection;
using TianShu.MQ.Core;

namespace TianShu.MQ.AspNetCore;

/// <summary>
/// IServiceCollection 扩展方法
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// 注册 TianShu MQ 服务
    /// </summary>
    public static IServiceCollection AddTianShuMQ(this IServiceCollection services, Action<TianShuMqOptions>? configure = null)
    {
        var options = new TianShuMqOptions();
        configure?.Invoke(options);

        // 注册单例 MessageQueue
        services.AddSingleton(sp =>
        {
            var queue = new MessageQueue();

            // 异步创建主题（在启动时同步执行）
            foreach (var topicOptions in options.Topics)
            {
                queue.CreateTopicAsync(topicOptions).GetAwaiter().GetResult();
            }

            return queue;
        });

        // 注册 Producer
        services.AddSingleton<IMessageProducer>(sp =>
        {
            var queue = sp.GetRequiredService<MessageQueue>();
            return new DefaultMessageProducer(queue);
        });

        // 注册 Consumer
        services.AddSingleton<IMessageConsumer>(sp =>
        {
            var queue = sp.GetRequiredService<MessageQueue>();
            return new DefaultMessageConsumer(queue);
        });

        // 注册 Admin
        services.AddSingleton<IAdminClient>(sp =>
        {
            var queue = sp.GetRequiredService<MessageQueue>();
            return new DefaultAdminClient(queue);
        });

        return services;
    }
}
