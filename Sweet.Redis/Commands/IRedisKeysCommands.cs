namespace Sweet.Redis
{
    public interface IRedisKeysCommands
    {
        long Del(string key, params string[] keys);

        byte[] Dump(string key);

        bool Exists(string key);
        bool Expire(string key, int seconds);
        bool ExpireAt(string key, int timestamp);

        string[] Keys(string pattern);

        bool Migrate(string host, int port, string key, int destinationDb,
                     long timeoutMs, bool copy, bool replace, params string[] keys);
        bool Move(string key, int db);

        long ObjectRefCount(string key);
        byte[] ObjectEncoding(string key);
        string ObjectEncodingString(string key);
        long ObjectIdleTime(string key);

        bool Persist(string key);

        bool PExpire(string key, long milliseconds);
        bool PExpireAt(string key, long millisecondsTimestamp);
        long PTtl(string key);

        string RandomKey();

        bool Rename(string key, string newKey);
        bool RenameNx(string key, string newKey);

        bool Restore(string key, long ttl, byte[] value);

        byte[][] Scan(int count = 10, string match = null);
        string[] ScanString(int count = 10, string match = null);

        byte[][] Sort(string key, bool descending, bool alpha = false,
                      int start = -1, int end = -1, string by = null, string get = null);

        long Touch(string key, params string[] keys);

        long Ttl(string key);

        string Type(string key);

        long Wait(int numberOfSlaves, int timeout);
    }
}
