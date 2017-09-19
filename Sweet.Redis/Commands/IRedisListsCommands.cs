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
    BLPOP key [key ...] timeout
    summary: Remove and get the first element in a list, or block until one is available
    since: 2.0.0

    BRPOP key [key ...] timeout
    summary: Remove and get the last element in a list, or block until one is available
    since: 2.0.0

    BRPOPLPUSH source destination timeout
    summary: Pop a value from a list, push it to another list and return it; or block until one is available
    since: 2.2.0

    LINDEX key index
    summary: Get an element from a list by its index
    since: 1.0.0

    LINSERT key BEFORE|AFTER pivot value
    summary: Insert an element before or after another element in a list
    since: 2.2.0

    LLEN key
    summary: Get the length of a list
    since: 1.0.0

    LPOP key
    summary: Remove and get the first element in a list
    since: 1.0.0

    LPUSH key value [value ...]
    summary: Prepend one or multiple values to a list
    since: 1.0.0

    LPUSHX key value
    summary: Prepend a value to a list, only if the list exists
    since: 2.2.0

    LRANGE key start stop
    summary: Get a range of elements from a list
    since: 1.0.0

    LREM key count value
    summary: Remove elements from a list
    since: 1.0.0

    LSET key index value
    summary: Set the value of an element in a list by its index
    since: 1.0.0

    LTRIM key start stop
    summary: Trim a list to the specified range
    since: 1.0.0

    RPOP key
    summary: Remove and get the last element in a list
    since: 1.0.0

    RPOPLPUSH source destination
    summary: Remove the last element in a list, prepend it to another list and return it
    since: 1.2.0

    RPUSH key value [value ...]
    summary: Append one or multiple values to a list
    since: 1.0.0

    RPUSHX key value
    summary: Append a value to a list, only if the list exists
    since: 2.2.0
     */
    public interface IRedisListsCommands
    {
        RedisMultiBytes BLPop(string key, int timeout);
        RedisMultiBytes BRPop(string key, int timeout);

        RedisBytes BRPopLPush(string source, string destination);
        RedisString BRPopLPushString(string source, string destination);

        RedisBytes LIndex(string key, int index);
        RedisString LIndexString(string key, int index);

        RedisBool LInsert(string key, bool insertBefore, byte[] pivot, byte[] value);

        RedisInt LLen(string key);

        RedisBytes LPop(string key);
        RedisString LPopString(string key);

        RedisInt LPush(string key, byte[] value);
        RedisInt LPush(string key, string value);
        RedisInt LPushX(string key, byte[] value);
        RedisInt LPushX(string key, string value);

        RedisMultiBytes LRange(string key, int start, int end);
        RedisMultiString LRangeString(string key, int start, int end);

        RedisInt LRem(string key, int count, byte[] value);
        RedisInt LRem(string key, int count, string value);

        RedisBool LSet(string key, int index, byte[] value);
        RedisBool LSet(string key, int index, string value);

        RedisBool LTrim(string key, int start, int end);

        RedisBytes RPop(string key);
        RedisBytes RPopLPush(string source, string destination);
        RedisString RPopLPushString(string source, string destination);
        RedisString RPopString(string key);

        RedisInt RPush(string key, byte[][] values);
        RedisInt RPush(string key, string[] values);
        RedisInt RPushX(string key, byte[] value);
        RedisInt RPushX(string key, string value);
    }

}
