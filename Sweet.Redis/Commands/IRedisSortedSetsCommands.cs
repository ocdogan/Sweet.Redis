namespace Sweet.Redis
{
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
