using System;

namespace TianShu.MQ.Serialization;

/// <summary>
/// 消息序列化器接口
/// </summary>
public interface IMessageSerializer
{
    /// <summary>将对象序列化为字节数组</summary>
    byte[] Serialize<T>(T obj);

    /// <summary>将字节数组反序列化为对象</summary>
    T Deserialize<T>(byte[] data);
}
