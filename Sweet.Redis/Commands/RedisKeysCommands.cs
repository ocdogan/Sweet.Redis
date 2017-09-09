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

            if (keys != null && keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.Del, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Del, keys.ToBytesArray()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[] Dump(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Dump, key.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public bool Exists(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Exists, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool Expire(string key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Expire, key.ToBytes(), seconds.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool ExpireAt(string key, int timestamp)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.ExpireAt, key.ToBytes(), timestamp.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public string[] Keys(string pattern)
        {
            if (pattern == null)
                throw new ArgumentNullException("pattern");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Keys, pattern.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public bool Migrate(string host, int port, string key, int destinationDb, long timeoutMs, bool copy = false, bool replace = false, params string[] keys)
        {
            if (host == null)
                throw new ArgumentNullException("host");

            if (key == null && (keys == null || keys.Length == 0))
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = host.ToBytes()
                                 .Merge(port.ToBytes())
                                 .Merge((key != null) ? key.ToBytes() : RedisCommands.EmptyString)
                                 .Merge(destinationDb.ToBytes())
                                 .Merge(timeoutMs.ToBytes());

            if (copy)
                parameters = parameters.Merge(RedisCommands.Copy);

            if (replace)
                parameters = parameters.Merge(RedisCommands.Replace);

            if (key == null)
                parameters = parameters
                                 .Merge(RedisCommands.Keys)
                                 .Merge(keys.ToBytesArray());

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Migrate, parameters))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool Move(string key, int db)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Move, key.ToBytes(), db.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public long ObjectRefCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Object, RedisCommands.RefCount, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[] ObjectEncoding(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Object, RedisCommands.Encoding, key.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public string ObjectEncodingString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Object, RedisCommands.Encoding, key.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public long ObjectIdleTime(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Object, RedisCommands.IdleTime, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public bool Persist(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Persist, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool PExpire(string key, long milliseconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.PExpire, key.ToBytes(), milliseconds.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool PExpireAt(string key, long millisecondsTimestamp)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.PExpireAt, key.ToBytes(), millisecondsTimestamp.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public long PTtl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.PTtl, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public string RandomKey()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RandomKey))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public bool Rename(string key, string newKey)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (newKey == null)
                throw new ArgumentNullException("newKey");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Rename, key.ToBytes(), newKey.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool RenameNx(string key, string newKey)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (newKey == null)
                throw new ArgumentNullException("newKey");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RenameNx, key.ToBytes(), newKey.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool Restore(string key, long ttl, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Rename, key.ToBytes(), ttl.ToBytes(), value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
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
                parameters = parameters.Merge(RedisCommands.Descending);

            if (alpha)
                parameters = parameters.Merge(RedisCommands.Alpha);

            if (start > -1 && end > -1)
                parameters = parameters
                    .Merge(RedisCommands.Limit)
                    .Merge(start.ToBytes())
                    .Merge(end.ToBytes());

            if (!String.IsNullOrEmpty(by))
                parameters = parameters
                    .Merge(RedisCommands.By)
                    .Merge(by.ToBytes());

            if (!String.IsNullOrEmpty(get))
                parameters = parameters
                    .Merge(RedisCommands.Get)
                    .Merge(get.ToBytes());

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Sort, parameters))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public long Touch(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys != null && keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(keys.ToBytesArray());

                using (var cmd = new RedisCommand(Db.Db, RedisCommands.Touch, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Touch, keys.ToBytesArray()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long Ttl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Ttl, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public string Type(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Type, key.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, true);
            }
        }

        public long Wait(int numberOfSlaves, int timeout)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Ttl, numberOfSlaves.ToBytes(), timeout.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        #endregion Methods
    }
}