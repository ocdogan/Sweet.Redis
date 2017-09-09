using System;
namespace Sweet.Redis
{
    public interface IRedisStringsCommands
    {
        long Append(string key, byte[] value);

        long BitCount(string key);
        long BitCount(string key, int start, int end);

        long Decr(string key);
        long DecrBy(string key, int count);
        long DecrBy(string key, long count);

        byte[] Get(string key);
        long GetBit(string key, int offset);
        byte[] GetRange(string key, int start, int end);
        string GetRangeString(string key, int start, int end);
        byte[] GetSet(string key, byte[] value);
        string GetSet(string key, string value);
        string GetString(string key);

        long Incr(string key);
        long IncrBy(string key, int count);
        long IncrBy(string key, long count);
        double IncrByFloat(string key, double increment);

        byte[][] MGet(params byte[][] keys);
        string[] MGet(params string[] keys);

        bool MSet(byte[][] keys, byte[][] values);
        bool MSet(string[] keys, string[] values);
        bool MSetNx(byte[][] keys, byte[][] values);
        bool MSetNx(string[] keys, string[] values);
        bool PSetEx(string key, long milliseconds, byte[] value);

        bool Set(string key, byte[] value);
        bool Set(string key, byte[] value, int expirySeconds, long expiryMilliseconds = 0L);
        bool Set(string key, string value);
        bool Set(string key, string value, int expirySeconds, long expiryMilliseconds = 0L);
        long SetBit(string key, int offset, int value);
        bool SetEx(string key, int seconds, byte[] value);
        bool SetEx(string key, int seconds, string value);
        bool SetNx(string key, byte[] value);
        bool SetNx(string key, string value);
        long SetRange(string key, int offset, byte[] value);

        long StrLen(string key);
    }
}
