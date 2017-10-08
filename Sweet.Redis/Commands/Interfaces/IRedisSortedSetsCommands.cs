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
    ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
    summary: Add one or more members to a sorted set, or update its score if it already exists
    since: 1.2.0

    ZCARD key
    summary: Get the number of members in a sorted set
    since: 1.2.0

    ZCOUNT key min max
    summary: Count the members in a sorted set with scores within the given values
    since: 2.0.0

    ZINCRBY key increment member
    summary: Increment the score of a member in a sorted set
    since: 1.2.0

    ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX]
    summary: Intersect multiple sorted sets and store the resulting sorted set in a new key
    since: 2.0.0

    ZLEXCOUNT key min max
    summary: Count the number of members in a sorted set between a given lexicographical range
    since: 2.8.9

    ZRANGE key start stop [WITHSCORES]
    summary: Return a range of members in a sorted set, by index
    since: 1.2.0

    ZRANGEBYLEX key min max [LIMIT offset count]
    summary: Return a range of members in a sorted set, by lexicographical range
    since: 2.8.9

    ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    summary: Return a range of members in a sorted set, by score
    since: 1.0.5

    ZRANK key member
    summary: Determine the index of a member in a sorted set
    since: 2.0.0

    ZREM key member [member ...]
    summary: Remove one or more members from a sorted set
    since: 1.2.0

    ZREMRANGEBYLEX key min max
    summary: Remove all members in a sorted set between the given lexicographical range
    since: 2.8.9

    ZREMRANGEBYRANK key start stop
    summary: Remove all members in a sorted set within the given indexes
    since: 2.0.0

    ZREMRANGEBYSCORE key min max
    summary: Remove all members in a sorted set within the given scores
    since: 1.2.0

    ZREVRANGE key start stop [WITHSCORES]
    summary: Return a range of members in a sorted set, by index, with scores ordered from high to low
    since: 1.2.0

    ZREVRANGEBYLEX key max min [LIMIT offset count]
    summary: Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
    since: 2.8.9

    ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    summary: Return a range of members in a sorted set, by score, with scores ordered from high to low
    since: 2.2.0

    ZREVRANK key member
    summary: Determine the index of a member in a sorted set, with scores ordered from high to low
    since: 2.0.0

    ZSCAN key cursor [MATCH pattern] [COUNT count]
    summary: Incrementally iterate sorted sets elements and associated scores
    since: 2.8.0

    ZSCORE key member
    summary: Get the score associated with the given member in a sorted set
    since: 1.2.0

    ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX]
    summary: Add multiple sorted sets and store the resulting sorted set in a new key
    since: 2.0.0
    */
    public interface IRedisSortedSetsCommands
    {
        RedisDouble ZAdd(RedisParam key, int score, RedisParam member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<int, RedisParam>[] scoresAndMembers);
        RedisDouble ZAdd(RedisParam key, long score, RedisParam member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<long, RedisParam>[] scoresAndMembers);
        RedisDouble ZAdd(RedisParam key, double score, RedisParam member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<double, RedisParam>[] scoresAndMembers);

        RedisInteger ZCard(RedisParam key);

        RedisInteger ZCount(RedisParam key, double min, double max);
        RedisInteger ZCount(RedisParam key, int min, int max);
        RedisInteger ZCount(RedisParam key, long min, long max);
        RedisInteger ZCount(RedisParam key, RedisParam min, RedisParam max);

        RedisDouble ZIncrBy(RedisParam key, double increment, RedisParam member);
        RedisDouble ZIncrBy(RedisParam key, int increment, RedisParam member);
        RedisDouble ZIncrBy(RedisParam key, long increment, RedisParam member);

        RedisInteger ZInterStore(RedisParam destination, int numkeys, RedisParam key, int weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<RedisParam, int>[] keysAndWeight);
        RedisInteger ZInterStore(RedisParam destination, int numkeys, RedisParam key, long weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<RedisParam, long>[] keysAndWeight);
        RedisInteger ZInterStore(RedisParam destination, int numkeys, RedisParam key, double weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<RedisParam, double>[] keysAndWeight);

        RedisInteger ZLexCount(RedisParam key, double min, double max);
        RedisInteger ZLexCount(RedisParam key, int min, int max);
        RedisInteger ZLexCount(RedisParam key, long min, long max);
        RedisInteger ZLexCount(RedisParam key, RedisParam min, RedisParam max);

        RedisMultiBytes ZRange(RedisParam key, double start, double stop);
        RedisMultiBytes ZRange(RedisParam key, int start, int stop);
        RedisMultiBytes ZRange(RedisParam key, long start, long stop);
        RedisMultiBytes ZRange(RedisParam key, RedisParam start, RedisParam stop);

        RedisMultiString ZRangeString(RedisParam key, double start, double stop);
        RedisMultiString ZRangeString(RedisParam key, int start, int stop);
        RedisMultiString ZRangeString(RedisParam key, long start, long stop);
        RedisMultiString ZRangeString(RedisParam key, RedisParam start, RedisParam stop);

        RedisMultiBytes ZRangeByLex(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByLex(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByLex(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByLex(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisMultiString ZRangeByLexString(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisMultiBytes ZRangeByScore(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByScore(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByScore(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByScore(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);
        
        RedisMultiString ZRangeByScoreString(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, double start, double stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, int start, int stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, long start, long stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, RedisParam start, RedisParam stop);

        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, double start, double stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, int start, int stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, long start, long stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, RedisParam start, RedisParam stop);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisNullableInteger ZRank(RedisParam key, RedisParam member);

        RedisInteger ZRem(RedisParam key, RedisParam member, params RedisParam[] members);

        RedisInteger ZRemRangeByLex(RedisParam key, double min, double max);
        RedisInteger ZRemRangeByLex(RedisParam key, int min, int max);
        RedisInteger ZRemRangeByLex(RedisParam key, long min, long max);
        RedisInteger ZRemRangeByLex(RedisParam key, RedisParam min, RedisParam max);

        RedisInteger ZRemRangeByRank(RedisParam key, RedisParam start, RedisParam stop);

        RedisInteger ZRemRangeByScore(RedisParam key, double min, double max);
        RedisInteger ZRemRangeByScore(RedisParam key, int min, int max);
        RedisInteger ZRemRangeByScore(RedisParam key, long min, long max);
        RedisInteger ZRemRangeByScore(RedisParam key, RedisParam min, RedisParam max);

        RedisMultiBytes ZRevRange(RedisParam key, double start, double stop);
        RedisMultiBytes ZRevRange(RedisParam key, int start, int stop);
        RedisMultiBytes ZRevRange(RedisParam key, long start, long stop);
        RedisMultiBytes ZRevRange(RedisParam key, RedisParam start, RedisParam stop);

        RedisMultiString ZRevRangeString(RedisParam key, double start, double stop);
        RedisMultiString ZRevRangeString(RedisParam key, int start, int stop);
        RedisMultiString ZRevRangeString(RedisParam key, long start, long stop);
        RedisMultiString ZRevRangeString(RedisParam key, RedisParam start, RedisParam stop);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, double start, double stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, int start, int stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, long start, long stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, RedisParam start, RedisParam stop);

        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, double start, double stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, int start, int stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, long start, long stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, RedisParam start, RedisParam stop);

        RedisMultiBytes ZRevRangeByScore(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRevRangeByScore(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRevRangeByScore(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRevRangeByScore(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisMultiString ZRevRangeByScoreString(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null);

        RedisNullableInteger ZRevRank(RedisParam key, RedisParam member);

        RedisMultiBytes ZScan(RedisParam key, int cursor, RedisParam? matchPattern = null, long? count = null);
        RedisMultiString ZScanString(RedisParam key, int cursor, RedisParam? matchPattern = null, long? count = null);
        RedisKeyValue<byte[], byte[]>[] ZScanKeyValue(RedisParam key, int cursor, RedisParam? matchPattern = null, long? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZScanKeyValueString(RedisParam key, int cursor, RedisParam? matchPattern = null, long? count = null);

        RedisDouble ZScore(RedisParam key, RedisParam member);

        RedisInteger ZUnionStore(RedisParam destination, int numkeys, RedisParam key, int weight, RedisAggregate aggregate = RedisAggregate.Default,
                    params RedisKeyValue<RedisParam, int>[] keysAndWeight);
        RedisInteger ZUnionStore(RedisParam destination, int numkeys, RedisParam key, long weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<RedisParam, long>[] keysAndWeight);
        RedisInteger ZUnionStore(RedisParam destination, int numkeys, RedisParam key, double weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<RedisParam, double>[] keysAndWeight);
    }
}
