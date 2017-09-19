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
        RedisDouble ZAdd(string key, int score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<int, string>[] scoresAndMembers);
        RedisDouble ZAdd(string key, int score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<int, byte[]>[] scoresAndMembers);
        RedisDouble ZAdd(string key, long score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<long, string>[] scoresAndMembers);
        RedisDouble ZAdd(string key, long score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<long, byte[]>[] scoresAndMembers);
        RedisDouble ZAdd(string key, double score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<double, string>[] scoresAndMembers);
        RedisDouble ZAdd(string key, double score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<double, byte[]>[] scoresAndMembers);

        RedisInt ZCard(string key);

        RedisInt ZCount(string key, int min, int max);
        RedisInt ZCount(string key, long min, long max);
        RedisInt ZCount(string key, double min, double max);

        RedisDouble ZIncrBy(string key, double increment, string member);
        RedisDouble ZIncrBy(string key, double increment, byte[] member);
        RedisDouble ZIncrBy(string key, int increment, string member);
        RedisDouble ZIncrBy(string key, int increment, byte[] member);
        RedisDouble ZIncrBy(string key, long increment, string member);
        RedisDouble ZIncrBy(string key, long increment, byte[] member);

        RedisInt ZInterStore(string destination, int numkeys, string key, int weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, int>[] keysAndWeight);
        RedisInt ZInterStore(string destination, int numkeys, string key, long weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, long>[] keysAndWeight);
        RedisInt ZInterStore(string destination, int numkeys, string key, double weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, double>[] keysAndWeight);

        RedisInt ZLexCount(string key, int min, int max);
        RedisInt ZLexCount(string key, long min, long max);
        RedisInt ZLexCount(string key, double min, double max);

        RedisMultiBytes ZRange(string key, double start, double stop);
        RedisMultiBytes ZRange(string key, int start, int stop);
        RedisMultiBytes ZRange(string key, long start, long stop);
        RedisMultiBytes ZRange(string key, string start, string stop);
        RedisMultiBytes ZRange(string key, byte[] start, byte[] stop);
        RedisMultiString ZRangeString(string key, double start, double stop);
        RedisMultiString ZRangeString(string key, int start, int stop);
        RedisMultiString ZRangeString(string key, long start, long stop);
        RedisMultiString ZRangeString(string key, string start, string stop);
        RedisMultiString ZRangeString(string key, byte[] start, byte[] stop);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, double start, double stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, int start, int stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, long start, long stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, string start, string stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, byte[] start, byte[] stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, double start, double stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, int start, int stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, long start, long stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, string start, string stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, byte[] start, byte[] stop);

        RedisMultiBytes ZRangeByLex(string key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByLex(string key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByLex(string key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByLex(string key, string start, string stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByLex(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(string key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(string key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(string key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(string key, string start, string stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByLexString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        RedisMultiBytes ZRangeByScore(string key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByScore(string key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByScore(string key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByScore(string key, string start, string stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRangeByScore(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(string key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(string key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(string key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(string key, string start, string stop, int? offset = null, int? count = null);
        RedisMultiString ZRangeByScoreString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, string start, string stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, string start, string stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        RedisNullableInt ZRank(string key, string member);
        RedisNullableInt ZRank(string key, byte[] member);

        RedisInt ZRem(string key, string member, params string[] members);
        RedisInt ZRem(string key, byte[] member, params byte[][] members);

        RedisInt ZRemRangeByLex(string key, double min, double max);
        RedisInt ZRemRangeByLex(string key, int min, int max);
        RedisInt ZRemRangeByLex(string key, long min, long max);
        RedisInt ZRemRangeByLex(string key, string min, string max);
        RedisInt ZRemRangeByLex(string key, byte[] min, byte[] max);

        RedisInt ZRemRangeByRank(string key, double start, double stop);
        RedisInt ZRemRangeByRank(string key, int start, int stop);
        RedisInt ZRemRangeByRank(string key, long start, long stop);
        RedisInt ZRemRangeByRank(string key, string start, string stop);
        RedisInt ZRemRangeByRank(string key, byte[] start, byte[] stop);

        RedisInt ZRemRangeByScore(string key, double min, double max);
        RedisInt ZRemRangeByScore(string key, int min, int max);
        RedisInt ZRemRangeByScore(string key, long min, long max);
        RedisInt ZRemRangeByScore(string key, string min, string max);
        RedisInt ZRemRangeByScore(string key, byte[] min, byte[] max);

        RedisMultiBytes ZRevRange(string key, double start, double stop);
        RedisMultiBytes ZRevRange(string key, int start, int stop);
        RedisMultiBytes ZRevRange(string key, long start, long stop);
        RedisMultiBytes ZRevRange(string key, string start, string stop);
        RedisMultiBytes ZRevRange(string key, byte[] start, byte[] stop);
        RedisMultiString ZRevRangeString(string key, double start, double stop);
        RedisMultiString ZRevRangeString(string key, int start, int stop);
        RedisMultiString ZRevRangeString(string key, long start, long stop);
        RedisMultiString ZRevRangeString(string key, string start, string stop);
        RedisMultiString ZRevRangeString(string key, byte[] start, byte[] stop);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, double start, double stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, int start, int stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, long start, long stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, string start, string stop);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, byte[] start, byte[] stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, double start, double stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, int start, int stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, long start, long stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, string start, string stop);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, byte[] start, byte[] stop);

        RedisMultiBytes ZRevRangeByScore(string key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRevRangeByScore(string key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRevRangeByScore(string key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRevRangeByScore(string key, string start, string stop, int? offset = null, int? count = null);
        RedisMultiBytes ZRevRangeByScore(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(string key, double start, double stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(string key, int start, int stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(string key, long start, long stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(string key, string start, string stop, int? offset = null, int? count = null);
        RedisMultiString ZRevRangeByScoreString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, string start, string stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, double start, double stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, int start, int stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, long start, long stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, string start, string stop, int? offset = null, int? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        RedisNullableInt ZRevRank(string key, string member);
        RedisNullableInt ZRevRank(string key, byte[] member);

        RedisMultiBytes ZScan(string key, int cursor, string matchPattern = null, long? count = null);
        RedisMultiString ZScanString(string key, int cursor, string matchPattern = null, long? count = null);
        RedisKeyValue<byte[], byte[]>[] ZScanKeyValue(string key, int cursor, string matchPattern = null, long? count = null);
        RedisResult<RedisKeyValue<string, double>[]> ZScanKeyValueString(string key, int cursor, string matchPattern = null, long? count = null);

        RedisDouble ZScore(string key, string member);

        RedisInt ZUnionStore(string destination, int numkeys, string key, int weight, RedisAggregate aggregate = RedisAggregate.Default,
                    params RedisKeyValue<string, int>[] keysAndWeight);
        RedisInt ZUnionStore(string destination, int numkeys, string key, long weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, long>[] keysAndWeight);
        RedisInt ZUnionStore(string destination, int numkeys, string key, double weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, double>[] keysAndWeight);
    }
}
