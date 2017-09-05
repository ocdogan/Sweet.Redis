namespace Sweet.Redis
{
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
