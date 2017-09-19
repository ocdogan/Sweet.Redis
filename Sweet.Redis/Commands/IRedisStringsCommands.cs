#region License
//  The MIT License (MIT)
//
//  Copyright (c) 2017, Cagatay Dogan
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//      The above copyright notice and this permission notice shall be included in
//      all copies or substantial portions of the Software.
//
//      THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//      IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//      FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//      AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//      LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//      OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//      THE SOFTWARE.
#endregion License

namespace Sweet.Redis
{
    /*
    APPEND key value
    summary: Append a value to a key
    since: 2.0.0

    BITCOUNT key [start end]
    summary: Count set bits in a string
    since: 2.6.0

    BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
    summary: Perform arbitrary bitfield integer operations on strings
    since: 3.2.0

    BITOP operation destkey key [key ...]
    summary: Perform bitwise operations between strings
    since: 2.6.0

    BITPOS key bit [start] [end]
    summary: Find first bit set or clear in a string
    since: 2.8.7

    DECR key
    summary: Decrement the integer value of a key by one
    since: 1.0.0

    DECRBY key decrement
    summary: Decrement the integer value of a key by the given number
    since: 1.0.0

    GET key
    summary: Get the value of a key
    since: 1.0.0

    GETBIT key offset
    summary: Returns the bit value at offset in the string value stored at key
    since: 2.2.0

    GETRANGE key start end
    summary: Get a substring of the string stored at a key
    since: 2.4.0

    GETSET key value
    summary: Set the string value of a key and return its old value
    since: 1.0.0

    INCR key
    summary: Increment the integer value of a key by one
    since: 1.0.0

    INCRBY key increment
    summary: Increment the integer value of a key by the given amount
    since: 1.0.0

    INCRBYFLOAT key increment
    summary: Increment the float value of a key by the given amount
    since: 2.6.0

    MGET key [key ...]
    summary: Get the values of all the given keys
    since: 1.0.0

    MSET key value [key value ...]
    summary: Set multiple keys to multiple values
    since: 1.0.1

    MSETNX key value [key value ...]
    summary: Set multiple keys to multiple values, only if none of the keys exist
    since: 1.0.1

    PSETEX key milliseconds value
    summary: Set the value and expiration in milliseconds of a key
    since: 2.6.0

    SET key value [EX seconds] [PX milliseconds] [NX|XX]
    summary: Set the string value of a key
    since: 1.0.0

    SETBIT key offset value
    summary: Sets or clears the bit at offset in the string value stored at key
    since: 2.2.0

    SETEX key seconds value
    summary: Set the value and expiration of a key
    since: 2.0.0

    SETNX key value
    summary: Set the value of a key, only if the key does not exist
    since: 1.0.0

    SETRANGE key offset value
    summary: Overwrite part of a string at key starting at the specified offset
    since: 2.2.0

    STRLEN key
    summary: Get the length of the value stored in a key
    since: 2.2.0
     */
    public interface IRedisStringsCommands
    {
        RedisInt Append(string key, byte[] value);

        RedisInt BitCount(string key);
        RedisInt BitCount(string key, int start, int end);

        RedisInt Decr(string key);
        RedisInt DecrBy(string key, int count);
        RedisInt DecrBy(string key, long count);

        RedisBytes Get(string key);
        RedisInt GetBit(string key, int offset);
        RedisBytes GetRange(string key, int start, int end);
        RedisString GetRangeString(string key, int start, int end);
        RedisBytes GetSet(string key, byte[] value);
        RedisString GetSet(string key, string value);
        RedisString GetString(string key);

        RedisInt Incr(string key);
        RedisInt IncrBy(string key, int count);
        RedisInt IncrBy(string key, long count);
        RedisDouble IncrByFloat(string key, double increment);

        RedisMultiBytes MGet(params byte[][] keys);
        RedisMultiString MGet(params string[] keys);

        RedisBool MSet(byte[][] keys, byte[][] values);
        RedisBool MSet(string[] keys, string[] values);
        RedisBool MSetNx(byte[][] keys, byte[][] values);
        RedisBool MSetNx(string[] keys, string[] values);
        RedisBool PSetEx(string key, long milliseconds, byte[] value);

        RedisBool Set(string key, byte[] value);
        RedisBool Set(string key, byte[] value, int expirySeconds, long expiryMilliseconds = RedisConstants.Zero);
        RedisBool Set(string key, string value);
        RedisBool Set(string key, string value, int expirySeconds, long expiryMilliseconds = RedisConstants.Zero);
        RedisInt SetBit(string key, int offset, int value);
        RedisBool SetEx(string key, int seconds, byte[] value);
        RedisBool SetEx(string key, int seconds, string value);
        RedisBool SetNx(string key, byte[] value);
        RedisBool SetNx(string key, string value);
        RedisInt SetRange(string key, int offset, byte[] value);

        RedisInt StrLen(string key);
    }
}
