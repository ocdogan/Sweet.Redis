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

using System;

namespace Sweet.Redis
{
    internal class RedisKeysCommands : RedisCommandSet, IRedisKeysCommands
    {
        #region .Ctors

        public RedisKeysCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInt Del(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectInteger(RedisCommands.Del, parameters);
            }
            return ExpectInteger(RedisCommands.Del, keys.ToBytesArray());
        }

        public RedisBytes Dump(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Dump, key.ToBytes());
        }

        public RedisBool Exists(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Exists, key.ToBytes());
        }

        public RedisBool Expire(string key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Expire, key.ToBytes(), seconds.ToBytes());
        }

        public RedisBool ExpireAt(string key, int timestamp)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.ExpireAt, key.ToBytes(), timestamp.ToBytes());
        }

        public RedisMultiString Keys(string pattern)
        {
            if (pattern == null)
                throw new ArgumentNullException("pattern");

            return ExpectMultiDataStrings(RedisCommands.Keys, pattern.ToBytes());
        }

        public RedisBool Migrate(string host, int port, string key, int destinationDb, long timeoutMs, bool copy = false, bool replace = false, params string[] keys)
        {
            if (host == null)
                throw new ArgumentNullException("host");

            if (key == null && (keys == null || keys.Length == 0))
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = host.ToBytes()
                                 .Join(port.ToBytes())
                                 .Join((key != null) ? key.ToBytes() : RedisCommands.EmptyString)
                                 .Join(destinationDb.ToBytes())
                                 .Join(timeoutMs.ToBytes());

            if (copy)
                parameters = parameters.Join(RedisCommands.Copy);

            if (replace)
                parameters = parameters.Join(RedisCommands.Replace);

            if (key == null)
                parameters = parameters
                                 .Join(RedisCommands.Keys)
                                 .Join(keys.ToBytesArray());

            return ExpectOK(RedisCommands.Migrate, parameters);
        }

        public RedisBool Move(string key, int db)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Move, key.ToBytes(), db.ToBytes());
        }

        public RedisInt ObjectRefCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Object, RedisCommands.RefCount, key.ToBytes());
        }

        public RedisBytes ObjectEncoding(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Object, RedisCommands.Encoding, key.ToBytes());
        }

        public RedisString ObjectEncodingString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.Object, RedisCommands.Encoding, key.ToBytes());
        }

        public RedisInt ObjectIdleTime(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Object, RedisCommands.IdleTime, key.ToBytes());
        }

        public RedisBool Persist(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Persist, key.ToBytes());
        }

        public RedisBool PExpire(string key, long milliseconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.PExpire, key.ToBytes(), milliseconds.ToBytes());
        }

        public RedisBool PExpireAt(string key, long millisecondsTimestamp)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.PExpireAt, key.ToBytes(), millisecondsTimestamp.ToBytes());
        }

        public RedisInt PTtl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.PTtl, key.ToBytes());
        }

        public RedisString RandomKey()
        {
            return ExpectBulkString(RedisCommands.RandomKey);
        }

        public RedisBool Rename(string key, string newKey)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (newKey == null)
                throw new ArgumentNullException("newKey");

            return ExpectOK(RedisCommands.Rename, key.ToBytes(), newKey.ToBytes());
        }

        public RedisBool RenameNx(string key, string newKey)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (newKey == null)
                throw new ArgumentNullException("newKey");

            return ExpectGreaterThanZero(RedisCommands.RenameNx, key.ToBytes(), newKey.ToBytes());
        }

        public RedisBool Restore(string key, long ttl, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.Rename, key.ToBytes(), ttl.ToBytes(), value);
        }

        public RedisMultiBytes Scan(int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public RedisMultiString ScanString(int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public RedisMultiBytes Sort(string key, bool descending, bool alpha = false,
                      int start = -1, int end = -1, string by = null, string get = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = new byte[1][] { key.ToBytes() };

            if (descending)
                parameters = parameters.Join(RedisCommands.Descending);

            if (alpha)
                parameters = parameters.Join(RedisCommands.Alpha);

            if (start > -1 && end > -1)
                parameters = parameters
                    .Join(RedisCommands.Limit)
                    .Join(start.ToBytes())
                    .Join(end.ToBytes());

            if (!String.IsNullOrEmpty(by))
                parameters = parameters
                    .Join(RedisCommands.By)
                    .Join(by.ToBytes());

            if (!String.IsNullOrEmpty(get))
                parameters = parameters
                    .Join(RedisCommands.Get)
                    .Join(get.ToBytes());

            return ExpectMultiDataBytes(RedisCommands.Sort, parameters);
        }

        public RedisInt Touch(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectInteger(RedisCommands.Touch, parameters);
            }
            return ExpectInteger(RedisCommands.Touch, keys.ToBytesArray());
        }

        public RedisInt Ttl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Ttl, key.ToBytes());
        }

        public RedisString Type(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectSimpleString(RedisCommands.Type, key.ToBytes());
        }

        public RedisInt Wait(int numberOfSlaves, int timeout)
        {
            return ExpectInteger(RedisCommands.Ttl, numberOfSlaves.ToBytes(), timeout.ToBytes());
        }

        #endregion Methods
    }
}
