﻿namespace Sweet.Redis
{
    /*
    PFADD key element [element ...]
    summary: Adds the specified elements to the specified HyperLogLog.
    since: 2.8.9

    PFCOUNT key [key ...]
    summary: Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
    since: 2.8.9

    PFMERGE destkey sourcekey [sourcekey ...]
    summary: Merge N different HyperLogLogs into a single one.
    since: 2.8.9
     */
    public interface IRedisHyperLogLogCommands
    {
        bool PfAdd(string key, string element, params string[] elements);
        long PfCount(string key, params string[] keys);
        bool PfMerge(string destKey, string sourceKey, params string[] sourceKeys);
    }
}