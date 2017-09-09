using System;
namespace Sweet.Redis
{
    public interface IRedisDisposable : IDisposable
    {
        bool Disposed { get; }
        void ValidateNotDisposed();
    }
}
