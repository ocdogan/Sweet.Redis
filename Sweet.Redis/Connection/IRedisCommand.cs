using System.IO;

namespace Sweet.Redis
{
    public interface IRedisCommand : IRedisDisposable
    {
        void WriteTo(Stream stream);
        void WriteTo(RedisSocket socket);
    }
}
