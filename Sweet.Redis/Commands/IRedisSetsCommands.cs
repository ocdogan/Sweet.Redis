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
    SADD key member [member ...]
    summary: Add one or more members to a set
    since: 1.0.0

    SCARD key
    summary: Get the number of members in a set
    since: 1.0.0

    SDIFF key [key ...]
    summary: Subtract multiple sets
    since: 1.0.0

    SDIFFSTORE destination key [key ...]
    summary: Subtract multiple sets and store the resulting set in a key
    since: 1.0.0

    SINTER key [key ...]
    summary: Intersect multiple sets
    since: 1.0.0

    SINTERSTORE destination key [key ...]
    summary: Intersect multiple sets and store the resulting set in a key
    since: 1.0.0

    SISMEMBER key member
    summary: Determine if a given value is a member of a set
    since: 1.0.0

    SMEMBERS key
    summary: Get all the members in a set
    since: 1.0.0

    SMOVE source destination member
    summary: Move a member from one set to another
    since: 1.0.0

    SPOP key [count]
    summary: Remove and return one or multiple random members from a set
    since: 1.0.0

    SRANDMEMBER key [count]
    summary: Get one or multiple random members from a set
    since: 1.0.0

    SREM key member [member ...]
    summary: Remove one or more members from a set
    since: 1.0.0

    SSCAN key cursor [MATCH pattern] [COUNT count]
    summary: Incrementally iterate Set elements
    since: 2.8.0

    SUNION key [key ...]
    summary: Add multiple sets
    since: 1.0.0

    SUNIONSTORE destination key [key ...]
    summary: Add multiple sets and store the resulting set in a key
    since: 1.0.0
     */
    public interface IRedisSetsCommands
    {
        RedisInt SAdd(RedisParam key, RedisParam member, params RedisParam[] members);

        RedisInt SCard(RedisParam key);

        RedisMultiBytes SDiff(RedisParam fromKey, params RedisParam[] keys);
        RedisInt SDiffStore(RedisParam toKey, RedisParam fromKey, params RedisParam[] keys);
        RedisMultiString SDiffString(RedisParam fromKey, params RedisParam[] keys);

        RedisMultiBytes SInter(RedisParam key, params RedisParam[] keys);
        RedisInt SInterStore(RedisParam toKey, params RedisParam[] keys);
        RedisMultiString SInterStrings(RedisParam key, params RedisParam[] keys);

        RedisBool SIsMember(RedisParam key, RedisParam member);

        RedisMultiBytes SMembers(RedisParam key);
        RedisMultiString SMemberStrings(RedisParam key);

        RedisBool SMove(RedisParam fromKey, RedisParam toKey, RedisParam member);

        RedisBytes SPop(RedisParam key);
        RedisString SPopString(RedisParam key);

        RedisBytes SRandMember(RedisParam key);
        RedisMultiBytes SRandMember(RedisParam key, int count);
        RedisString SRandMemberString(RedisParam key);
        RedisMultiString SRandMemberString(RedisParam key, int count);

        RedisInt SRem(RedisParam key, RedisParam member, params RedisParam[] members);

        RedisMultiBytes SScan(RedisParam key, int count = 10, RedisParam? match = null);
        RedisMultiString SScanString(RedisParam key, int count = 10, RedisParam? match = null);

        RedisMultiBytes SUnion(RedisParam key, params RedisParam[] keys);
        RedisInt SUnionStore(RedisParam intoKey, params RedisParam[] keys);
        RedisMultiString SUnionStrings(RedisParam key, params RedisParam[] keys);
    }
}
