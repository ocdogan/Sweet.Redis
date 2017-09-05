using System;

namespace Sweet.Redis
{
    public interface IRedisResponseReader : IDisposable
    {
        bool Executing { get; }
        IRedisResponse Execute();
    }
}
