﻿using System;

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

        public long Del(string key, params string[] keys)
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

        public byte[] Dump(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Dump, key.ToBytes());
        }

        public bool Exists(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Exists, key.ToBytes());
        }

        public bool Expire(string key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Expire, key.ToBytes(), seconds.ToBytes());
        }

        public bool ExpireAt(string key, int timestamp)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.ExpireAt, key.ToBytes(), timestamp.ToBytes());
        }

        public string[] Keys(string pattern)
        {
            if (pattern == null)
                throw new ArgumentNullException("pattern");

            return ExpectMultiDataStrings(RedisCommands.Keys, pattern.ToBytes());
        }

        public bool Migrate(string host, int port, string key, int destinationDb, long timeoutMs, bool copy = false, bool replace = false, params string[] keys)
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

        public bool Move(string key, int db)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Move, key.ToBytes(), db.ToBytes());
        }

        public long ObjectRefCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Object, RedisCommands.RefCount, key.ToBytes());
        }

        public byte[] ObjectEncoding(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Object, RedisCommands.Encoding, key.ToBytes());
        }

        public string ObjectEncodingString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.Object, RedisCommands.Encoding, key.ToBytes());
        }

        public long ObjectIdleTime(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Object, RedisCommands.IdleTime, key.ToBytes());
        }

        public bool Persist(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.Persist, key.ToBytes());
        }

        public bool PExpire(string key, long milliseconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.PExpire, key.ToBytes(), milliseconds.ToBytes());
        }

        public bool PExpireAt(string key, long millisecondsTimestamp)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.PExpireAt, key.ToBytes(), millisecondsTimestamp.ToBytes());
        }

        public long PTtl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.PTtl, key.ToBytes());
        }

        public string RandomKey()
        {
            return ExpectBulkString(RedisCommands.RandomKey);
        }

        public bool Rename(string key, string newKey)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (newKey == null)
                throw new ArgumentNullException("newKey");

            return ExpectOK(RedisCommands.Rename, key.ToBytes(), newKey.ToBytes());
        }

        public bool RenameNx(string key, string newKey)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (newKey == null)
                throw new ArgumentNullException("newKey");

            return ExpectGreaterThanZero(RedisCommands.RenameNx, key.ToBytes(), newKey.ToBytes());
        }

        public bool Restore(string key, long ttl, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.Rename, key.ToBytes(), ttl.ToBytes(), value);
        }

        public byte[][] Scan(int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public string[] ScanString(int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public byte[][] Sort(string key, bool descending, bool alpha = false,
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

        public long Touch(string key, params string[] keys)
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

        public long Ttl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Ttl, key.ToBytes());
        }

        public string Type(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectSimpleString(RedisCommands.Type, key.ToBytes());
        }

        public long Wait(int numberOfSlaves, int timeout)
        {
            return ExpectInteger(RedisCommands.Ttl, numberOfSlaves.ToBytes(), timeout.ToBytes());
        }

        #endregion Methods
    }
}
