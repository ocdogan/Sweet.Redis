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
        byte[][] BLPop(string key, int timeout);
        byte[][] BRPop(string key, int timeout);

        byte[] BRPopLPush(string source, string destination);
        string BRPopLPushString(string source, string destination);

        byte[] LIndex(string key, int index);
        string LIndexString(string key, int index);

        bool LInsert(string key, bool insertBefore, byte[] pivot, byte[] value);

        long LLen(string key);

        byte[] LPop(string key);
        string LPopString(string key);

        long LPush(string key, byte[] value);
        long LPush(string key, string value);
        long LPushX(string key, byte[] value);
        long LPushX(string key, string value);

        byte[][] LRange(string key, int start, int end);
        string[] LRangeString(string key, int start, int end);

        long LRem(string key, int count, byte[] value);
        long LRem(string key, int count, string value);

        bool LSet(string key, int index, byte[] value);
        bool LSet(string key, int index, string value);

        bool LTrim(string key, int start, int end);

        byte[] RPop(string key);
        byte[] RPopLPush(string source, string destination);
        string RPopLPushString(string source, string destination);
        string RPopString(string key);

        long RPush(string key, byte[][] values);
        long RPush(string key, string[] values);
        long RPushX(string key, byte[] value);
        long RPushX(string key, string value);
    }

}
