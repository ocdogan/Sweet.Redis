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
        double ZAdd(string key, int score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<int, string>[] scoresAndMembers);
        double ZAdd(string key, int score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<int, byte[]>[] scoresAndMembers);
        double ZAdd(string key, long score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<long, string>[] scoresAndMembers);
        double ZAdd(string key, long score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<long, byte[]>[] scoresAndMembers);
        double ZAdd(string key, double score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<double, string>[] scoresAndMembers);
        double ZAdd(string key, double score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                    bool changed = false, bool increment = false, params RedisKeyValue<double, byte[]>[] scoresAndMembers);

        long ZCard(string key);

        long ZCount(string key, int min, int max);
        long ZCount(string key, long min, long max);
        long ZCount(string key, double min, double max);

        double ZIncrBy(string key, double increment, string member);
        double ZIncrBy(string key, double increment, byte[] member);
        double ZIncrBy(string key, int increment, string member);
        double ZIncrBy(string key, int increment, byte[] member);
        double ZIncrBy(string key, long increment, string member);
        double ZIncrBy(string key, long increment, byte[] member);

        long ZInterStore(string destination, int numkeys, string key, int weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, int>[] keysAndWeight);
        long ZInterStore(string destination, int numkeys, string key, long weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, long>[] keysAndWeight);
        long ZInterStore(string destination, int numkeys, string key, double weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, double>[] keysAndWeight);

        long ZLexCount(string key, int min, int max);
        long ZLexCount(string key, long min, long max);
        long ZLexCount(string key, double min, double max);

        byte[][] ZRange(string key, double start, double stop);
        byte[][] ZRange(string key, int start, int stop);
        byte[][] ZRange(string key, long start, long stop);
        byte[][] ZRange(string key, string start, string stop);
        byte[][] ZRange(string key, byte[] start, byte[] stop);
        string[] ZRangeString(string key, double start, double stop);
        string[] ZRangeString(string key, int start, int stop);
        string[] ZRangeString(string key, long start, long stop);
        string[] ZRangeString(string key, string start, string stop);
        string[] ZRangeString(string key, byte[] start, byte[] stop);

        RedisKeyValue<byte[], double>[] ZRangeWithScores(string key, double start, double stop);
        RedisKeyValue<byte[], double>[] ZRangeWithScores(string key, int start, int stop);
        RedisKeyValue<byte[], double>[] ZRangeWithScores(string key, long start, long stop);
        RedisKeyValue<byte[], double>[] ZRangeWithScores(string key, string start, string stop);
        RedisKeyValue<byte[], double>[] ZRangeWithScores(string key, byte[] start, byte[] stop);
        RedisKeyValue<string, double>[] ZRangeWithScoresString(string key, double start, double stop);
        RedisKeyValue<string, double>[] ZRangeWithScoresString(string key, int start, int stop);
        RedisKeyValue<string, double>[] ZRangeWithScoresString(string key, long start, long stop);
        RedisKeyValue<string, double>[] ZRangeWithScoresString(string key, string start, string stop);
        RedisKeyValue<string, double>[] ZRangeWithScoresString(string key, byte[] start, byte[] stop);

        byte[][] ZRangeByLex(string key, double start, double stop, int? offset = null, int? count = null);
        byte[][] ZRangeByLex(string key, int start, int stop, int? offset = null, int? count = null);
        byte[][] ZRangeByLex(string key, long start, long stop, int? offset = null, int? count = null);
        byte[][] ZRangeByLex(string key, string start, string stop, int? offset = null, int? count = null);
        byte[][] ZRangeByLex(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        string[] ZRangeByLexString(string key, double start, double stop, int? offset = null, int? count = null);
        string[] ZRangeByLexString(string key, int start, int stop, int? offset = null, int? count = null);
        string[] ZRangeByLexString(string key, long start, long stop, int? offset = null, int? count = null);
        string[] ZRangeByLexString(string key, string start, string stop, int? offset = null, int? count = null);
        string[] ZRangeByLexString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        byte[][] ZRangeByScore(string key, double start, double stop, int? offset = null, int? count = null);
        byte[][] ZRangeByScore(string key, int start, int stop, int? offset = null, int? count = null);
        byte[][] ZRangeByScore(string key, long start, long stop, int? offset = null, int? count = null);
        byte[][] ZRangeByScore(string key, string start, string stop, int? offset = null, int? count = null);
        byte[][] ZRangeByScore(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        string[] ZRangeByScoreString(string key, double start, double stop, int? offset = null, int? count = null);
        string[] ZRangeByScoreString(string key, int start, int stop, int? offset = null, int? count = null);
        string[] ZRangeByScoreString(string key, long start, long stop, int? offset = null, int? count = null);
        string[] ZRangeByScoreString(string key, string start, string stop, int? offset = null, int? count = null);
        string[] ZRangeByScoreString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        RedisKeyValue<byte[], double>[] ZRangeByScoreWithScores(string key, double start, double stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRangeByScoreWithScores(string key, int start, int stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRangeByScoreWithScores(string key, long start, long stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRangeByScoreWithScores(string key, string start, string stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRangeByScoreWithScores(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRangeByScoreWithScoresString(string key, double start, double stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRangeByScoreWithScoresString(string key, int start, int stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRangeByScoreWithScoresString(string key, long start, long stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRangeByScoreWithScoresString(string key, string start, string stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRangeByScoreWithScoresString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        long? ZRank(string key, string member);
        long? ZRank(string key, byte[] member);

        long ZRem(string key, string member, params string[] members);
        long ZRem(string key, byte[] member, params byte[][] members);

        long ZRemRangeByLex(string key, double min, double max);
        long ZRemRangeByLex(string key, int min, int max);
        long ZRemRangeByLex(string key, long min, long max);
        long ZRemRangeByLex(string key, string min, string max);
        long ZRemRangeByLex(string key, byte[] min, byte[] max);

        long ZRemRangeByRank(string key, double start, double stop);
        long ZRemRangeByRank(string key, int start, int stop);
        long ZRemRangeByRank(string key, long start, long stop);
        long ZRemRangeByRank(string key, string start, string stop);
        long ZRemRangeByRank(string key, byte[] start, byte[] stop);

        long ZRemRangeByScore(string key, double min, double max);
        long ZRemRangeByScore(string key, int min, int max);
        long ZRemRangeByScore(string key, long min, long max);
        long ZRemRangeByScore(string key, string min, string max);
        long ZRemRangeByScore(string key, byte[] min, byte[] max);

        byte[][] ZRevRange(string key, double start, double stop);
        byte[][] ZRevRange(string key, int start, int stop);
        byte[][] ZRevRange(string key, long start, long stop);
        byte[][] ZRevRange(string key, string start, string stop);
        byte[][] ZRevRange(string key, byte[] start, byte[] stop);
        string[] ZRevRangeString(string key, double start, double stop);
        string[] ZRevRangeString(string key, int start, int stop);
        string[] ZRevRangeString(string key, long start, long stop);
        string[] ZRevRangeString(string key, string start, string stop);
        string[] ZRevRangeString(string key, byte[] start, byte[] stop);

        RedisKeyValue<byte[], double>[] ZRevRangeWithScores(string key, double start, double stop);
        RedisKeyValue<byte[], double>[] ZRevRangeWithScores(string key, int start, int stop);
        RedisKeyValue<byte[], double>[] ZRevRangeWithScores(string key, long start, long stop);
        RedisKeyValue<byte[], double>[] ZRevRangeWithScores(string key, string start, string stop);
        RedisKeyValue<byte[], double>[] ZRevRangeWithScores(string key, byte[] start, byte[] stop);
        RedisKeyValue<string, double>[] ZRevRangeWithScoresString(string key, double start, double stop);
        RedisKeyValue<string, double>[] ZRevRangeWithScoresString(string key, int start, int stop);
        RedisKeyValue<string, double>[] ZRevRangeWithScoresString(string key, long start, long stop);
        RedisKeyValue<string, double>[] ZRevRangeWithScoresString(string key, string start, string stop);
        RedisKeyValue<string, double>[] ZRevRangeWithScoresString(string key, byte[] start, byte[] stop);

        byte[][] ZRevRangeByScore(string key, double start, double stop, int? offset = null, int? count = null);
        byte[][] ZRevRangeByScore(string key, int start, int stop, int? offset = null, int? count = null);
        byte[][] ZRevRangeByScore(string key, long start, long stop, int? offset = null, int? count = null);
        byte[][] ZRevRangeByScore(string key, string start, string stop, int? offset = null, int? count = null);
        byte[][] ZRevRangeByScore(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        string[] ZRevRangeByScoreString(string key, double start, double stop, int? offset = null, int? count = null);
        string[] ZRevRangeByScoreString(string key, int start, int stop, int? offset = null, int? count = null);
        string[] ZRevRangeByScoreString(string key, long start, long stop, int? offset = null, int? count = null);
        string[] ZRevRangeByScoreString(string key, string start, string stop, int? offset = null, int? count = null);
        string[] ZRevRangeByScoreString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        RedisKeyValue<byte[], double>[] ZRevRangeByScoreWithScores(string key, double start, double stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRevRangeByScoreWithScores(string key, int start, int stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRevRangeByScoreWithScores(string key, long start, long stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRevRangeByScoreWithScores(string key, string start, string stop, int? offset = null, int? count = null);
        RedisKeyValue<byte[], double>[] ZRevRangeByScoreWithScores(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRevRangeByScoreWithScoresString(string key, double start, double stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRevRangeByScoreWithScoresString(string key, int start, int stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRevRangeByScoreWithScoresString(string key, long start, long stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRevRangeByScoreWithScoresString(string key, string start, string stop, int? offset = null, int? count = null);
        RedisKeyValue<string, double>[] ZRevRangeByScoreWithScoresString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null);

        long? ZRevRank(string key, string member);
        long? ZRevRank(string key, byte[] member);

        byte[][] ZScan(string key, int cursor, string matchPattern = null, long? count = null);
        string[] ZScanString(string key, int cursor, string matchPattern = null, long? count = null);
        RedisKeyValue<byte[], byte[]>[] ZScanKeyValue(string key, int cursor, string matchPattern = null, long? count = null);
        RedisKeyValue<string, double>[] ZScanKeyValueString(string key, int cursor, string matchPattern = null, long? count = null);

        double ZScore(string key, string member);

        long ZUnionStore(string destination, int numkeys, string key, int weight, RedisAggregate aggregate = RedisAggregate.Default,
                    params RedisKeyValue<string, int>[] keysAndWeight);
        long ZUnionStore(string destination, int numkeys, string key, long weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, long>[] keysAndWeight);
        long ZUnionStore(string destination, int numkeys, string key, double weight, RedisAggregate aggregate = RedisAggregate.Default,
                        params RedisKeyValue<string, double>[] keysAndWeight);
    }
}
