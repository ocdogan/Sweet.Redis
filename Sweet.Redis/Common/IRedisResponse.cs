using System;
using System.Collections.Generic;

namespace Sweet.Redis
{
    public interface IRedisResponse : IDisposable
    {
        int ChildCount { get; }
        byte[] Data { get; }
        bool HasChild { get; }
        bool HasData { get; }
        IList<IRedisResponse> Items { get; }
        int Length { get; }
        IRedisResponse Parent { get; }
        bool Ready { get; }
        RedisObjectType Type { get; }
        int TypeByte { get; }

        void Clear();
        byte[] ReleaseData();
    }
}
