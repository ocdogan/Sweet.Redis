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
    DEL key [key ...]
    summary: Delete a key
    since: 1.0.0

    DUMP key
    summary: Return a serialized version of the value stored at the specified key.
    since: 2.6.0

    EXISTS key [key ...]
    summary: Determine if a key exists
    since: 1.0.0

    EXPIRE key seconds
    summary: Set a key's time to live in seconds
    since: 1.0.0

    EXPIREAT key timestamp
    summary: Set the expiration for a key as a UNIX timestamp
    since: 1.2.0

    KEYS pattern
    summary: Find all keys matching the given pattern
    since: 1.0.0

    MIGRATE host port key| destination-db timeout [COPY] [REPLACE] [KEYS key]
    summary: Atomically transfer a key from a Redis instance to another one.
    since: 2.6.0

    MOVE key db
    summary: Move a key to another database
    since: 1.0.0

    OBJECT subcommand [arguments [arguments ...]]
    summary: Inspect the internals of Redis objects
    since: 2.2.3

    PERSIST key
    summary: Remove the expiration from a key
    since: 2.2.0

    PEXPIRE key milliseconds
    summary: Set a key's time to live in milliseconds
    since: 2.6.0

    PEXPIREAT key milliseconds-timestamp
    summary: Set the expiration for a key as a UNIX timestamp specified in milliseconds
    since: 2.6.0

    PTTL key
    summary: Get the time to live for a key in milliseconds
    since: 2.6.0

    RANDOMKEY -
    summary: Return a random key from the keyspace
    since: 1.0.0

    RENAME key newkey
    summary: Rename a key
    since: 1.0.0

    RENAMENX key newkey
    summary: Rename a key, only if the new key does not exist
    since: 1.0.0

    RESTORE key ttl serialized-value [REPLACE]
    summary: Create a key using the provided serialized value, previously obtained using DUMP.
    since: 2.6.0

    SCAN cursor [MATCH pattern] [COUNT count]
    summary: Incrementally iterate the keys space
    since: 2.8.0

    SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
    summary: Sort the elements in a list, set or sorted set
    since: 1.0.0

    TOUCH key arg ...options...
    summary: Help not available
    since: not known

    TTL key
    summary: Get the time to live for a key
    since: 1.0.0

    TYPE key
    summary: Determine the type stored at key
    since: 1.0.0

    UNLINK key arg ...options...
    summary: Help not available
    since: not known

    WAIT numslaves timeout
    summary: Wait for the synchronous replication of all the write commands sent in the context of the current connection
    since: 3.0.0
     */
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
