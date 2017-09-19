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

using System.Collections;
using System.Collections.Generic;

namespace Sweet.Redis
{
    /*
    HDEL key field [field ...]
    summary: Delete one or more hash fields
    since: 2.0.0

    HEXISTS key field
    summary: Determine if a hash field exists
    since: 2.0.0

    HGET key field
    summary: Get the value of a hash field
    since: 2.0.0

    HGETALL key
    summary: Get all the fields and values in a hash
    since: 2.0.0

    HINCRBY key field increment
    summary: Increment the integer value of a hash field by the given number
    since: 2.0.0

    HINCRBYFLOAT key field increment
    summary: Increment the float value of a hash field by the given amount
    since: 2.6.0

    HKEYS key
    summary: Get all the fields in a hash
    since: 2.0.0

    HLEN key
    summary: Get the number of fields in a hash
    since: 2.0.0

    HMGET key field [field ...]
    summary: Get the values of all the given hash fields
    since: 2.0.0

    HMSET key field value [field value ...]
    summary: Set multiple hash fields to multiple values
    since: 2.0.0

    HSCAN key cursor [MATCH pattern] [COUNT count]
    summary: Incrementally iterate hash fields and associated values
    since: 2.8.0

    HSET key field value
    summary: Set the string value of a hash field
    since: 2.0.0

    HSETNX key field value
    summary: Set the value of a hash field, only if the field does not exist
    since: 2.0.0

    HSTRLEN key field
    summary: Get the length of the value of a hash field
    since: 3.2.0

    HVALS key
    summary: Get all the values in a hash
    since: 2.0.0
     */
    public interface IRedisHashesCommands
    {
        RedisInt HDel(string key, byte[] field, params byte[][] fields);
        RedisInt HDel(string key, string field, params string[] fields);

        RedisBool HExists(string key, byte[] field);
        RedisBool HExists(string key, string field);

        RedisBytes HGet(string key, byte[] field);
        RedisBytes HGet(string key, string field);

        RedisMultiBytes HGetAll(string key);

        RedisResult<Hashtable> HGetAllHashtable(string key);
        RedisResult<Dictionary<string, string>> HGetAllDictionary(string key);

        RedisString HGetString(string key, string field);

        RedisInt HIncrBy(string key, byte[] field, int increment);
        RedisInt HIncrBy(string key, byte[] field, long increment);
        RedisDouble HIncrByFloat(string key, byte[] field, double increment);

        RedisMultiBytes HKeys(string key);
        RedisMultiString HKeyStrings(string key);

        RedisInt HLen(string key);

        RedisMultiBytes HMGet(string key, byte[] field, params byte[][] fields);
        RedisMultiBytes HMGet(string key, string field, params string[] fields);
        RedisMultiString HMGetStrings(string key, string field, params string[] fields);

        RedisBool HMSet(string key, byte[] field, byte[] value, byte[][] fields = null, byte[][] values = null);
        RedisBool HMSet(string key, string field, byte[] value, string[] fields = null, byte[][] values = null);
        RedisBool HMSet(string key, string field, string value, string[] fields = null, string[] values = null);
        RedisBool HMSet(string key, Hashtable values);
        RedisBool HMSet(string key, Dictionary<string, string> values);

        RedisMultiBytes HScan(string key, int count = 10, string match = null);
        RedisMultiString HScanString(string key, int count = 10, string match = null);

        RedisBool HSet(string key, byte[] field, byte[] value);
        RedisBool HSet(string key, string field, byte[] value);
        RedisBool HSet(string key, string field, string value);

        RedisBool HSetNx(string key, byte[] field, byte[] value);
        RedisBool HSetNx(string key, string field, byte[] value);
        RedisBool HSetNx(string key, string field, string value);

        RedisInt HStrLen(string key, byte[] field);
        RedisInt HStrLen(string key, string field);

        RedisMultiBytes HVals(string key);
        RedisMultiString HValStrings(string key);
    }
}
