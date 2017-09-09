namespace Sweet.Redis
{
    public interface IRedisHyperLogLogCommands
    {
        bool PfAdd(string key, string element, params string[] elements);
        long PfCount(string key, params string[] keys);
        bool PfMerge(string destKey, string sourceKey, params string[] sourceKeys);
    }
}