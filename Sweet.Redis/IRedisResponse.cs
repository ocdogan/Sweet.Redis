using System;
using System.Collections.Generic;

namespace Sweet.Redis
{
    public interface IRedisResponse : IDisposable
    {
        int Count { get; }
        byte[] Data { get; }
        IList<IRedisResponse> Items { get; }
        int Length { get; }
        IRedisResponse Parent { get; }
        bool Ready { get; }
        RedisObjectType Type { get; }

        void Clear();
        byte[] ReleaseData();
    }
}
