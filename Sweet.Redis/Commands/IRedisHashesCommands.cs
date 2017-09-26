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
        RedisInt HDel(RedisParam key, RedisParam field, params RedisParam[] fields);

        RedisBool HExists(RedisParam key, RedisParam field);

        RedisBytes HGet(RedisParam key, RedisParam field);

        RedisMultiBytes HGetAll(RedisParam key);
        RedisMultiString HGetAllString(RedisParam key);

        RedisResult<Hashtable> HGetAllHashtable(RedisParam key);
        RedisResult<Dictionary<string, string>> HGetAllDictionary(RedisParam key);

        RedisString HGetString(RedisParam key, RedisParam field);

        RedisInt HIncrBy(RedisParam key, RedisParam field, int increment);
        RedisInt HIncrBy(RedisParam key, RedisParam field, long increment);
        RedisDouble HIncrByFloat(RedisParam key, RedisParam field, double increment);

        RedisMultiBytes HKeys(RedisParam key);
        RedisMultiString HKeyStrings(RedisParam key);

        RedisInt HLen(RedisParam key);

        RedisMultiBytes HMGet(RedisParam key, RedisParam field, params RedisParam[] fields);
        RedisMultiString HMGetStrings(RedisParam key, RedisParam field, params RedisParam[] fields);

        RedisBool HMSet(RedisParam key, RedisParam field, RedisParam value, RedisParam[] fields = null, RedisParam[] values = null);
        RedisBool HMSet(RedisParam key, Hashtable values);
        RedisBool HMSet(RedisParam key, IDictionary<string, string> values);
        RedisBool HMSet(RedisParam key, IDictionary<RedisParam, RedisParam> values);

        RedisMultiBytes HScan(RedisParam key, int count = 10, RedisParam? match = null);
        RedisMultiString HScanString(RedisParam key, int count = 10, RedisParam? match = null);

        RedisBool HSet(RedisParam key, RedisParam field, RedisParam value);

        RedisBool HSetNx(RedisParam key, RedisParam field, RedisParam value);

        RedisInt HStrLen(RedisParam key, RedisParam field);

        RedisMultiBytes HVals(RedisParam key);
        RedisMultiString HValStrings(RedisParam key);
    }
}
