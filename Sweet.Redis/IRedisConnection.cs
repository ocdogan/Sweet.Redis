using System;

namespace Sweet.Redis
{
    public interface IRedisConnection : IDisposable
    {
        bool Disposed { get; }
        long LastError { get; }
        string Name { get; }
        RedisSettings Settings { get; }
        RedisConnectionState State { get; }

        IRedisResponse Send(byte[] data);
    }
}
