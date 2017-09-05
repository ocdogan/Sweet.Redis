using System.Collections;
using System.Collections.Generic;

namespace Sweet.Redis
{
    public interface IRedisHashesCommands
    {
        long HDel(string key, byte[] field, params byte[][] fields);
        long HDel(string key, string field, params string[] fields);

        bool HExists(string key, byte[] field);
        bool HExists(string key, string field);

        byte[] HGet(string key, byte[] field);
        byte[] HGet(string key, string field);

        byte[][] HGetAll(string key);

        Hashtable HGetAllHashtable(string key);
        Dictionary<string, string> HGetAllDictionary(string key);

        string HGetString(string key, string field);

        long HIncrBy(string key, byte[] field, int increment);
        long HIncrBy(string key, byte[] field, long increment);
        double HIncrByFloat(string key, byte[] field, double increment);

        byte[][] HKeys(string key);
        string[] HKeyStrings(string key);

        long HLen(string key);

        byte[][] HMGet(string key, byte[] field, params byte[][] fields);
        byte[][] HMGet(string key, string field, params string[] fields);
        string[] HMGetStrings(string key, string field, params string[] fields);

        bool HMSet(string key, byte[] field, byte[] value, byte[][] fields = null, byte[][] values = null);
        bool HMSet(string key, string field, byte[] value, string[] fields = null, byte[][] values = null);
        bool HMSet(string key, string field, string value, string[] fields = null, string[] values = null);
        bool HMSet(string key, Hashtable values);
        bool HMSet(string key, Dictionary<string, string> values);

        byte[][] HScan(string key, int count = 10, string match = null);
        string[] HScanString(string key, int count = 10, string match = null);

        bool HSet(string key, byte[] field, byte[] value);
        bool HSet(string key, string field, byte[] value);
        bool HSet(string key, string field, string value);

        bool HSetNx(string key, byte[] field, byte[] value);
        bool HSetNx(string key, string field, byte[] value);
        bool HSetNx(string key, string field, string value);

        long HStrLen(string key, byte[] field);
        long HStrLen(string key, string field);

        byte[][] HVals(string key);
        string[] HValStrings(string key);
    }
}
