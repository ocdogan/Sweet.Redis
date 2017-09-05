using System;
using System.Text;

namespace Sweet.Redis
{
    internal class RedisStringsCommands : RedisCommandSet, IRedisStringsCommands
    {
        #region .Ctors

        public RedisStringsCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public long Append(string key, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Append, key.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long BitCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.BitCount, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long BitCount(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.BitCount, key.ToBytes(), start.ToBytes(), end.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long Decr(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Decr, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long DecrBy(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.DecrBy, key.ToBytes(), count.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long DecrBy(string key, long count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.DecrBy, key.ToBytes(), count.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[] Get(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Get, key.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public long GetBit(string key, int offset)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.GetBit, key.ToBytes(), offset.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[] GetRange(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.GetRange, key.ToBytes(), start.ToBytes(), end.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public string GetRangeString(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.GetRange, key.ToBytes(), start.ToBytes(), end.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public byte[] GetSet(string key, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.GetSet, key.ToBytes(), value))
            {
                var result = cmd.ExpectBulkString(Db.Pool, true);
                if (result != null)
                    return Encoding.UTF8.GetBytes(result);
                return null;
            }
        }

        public string GetSet(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.GetSet, key.ToBytes(), bytes))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public string GetString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Get, key.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public long Incr(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Incr, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long IncrBy(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.IncrBy, key.ToBytes(), count.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long IncrBy(string key, long count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.IncrBy, key.ToBytes(), count.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public double IncrByFloat(string key, double increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.IncrBy, key.ToBytes(), increment.ToBytes()))
            {
                return cmd.ExpectDouble(Db.Pool, true);
            }
        }

        public byte[][] MGet(params byte[][] keys)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.MGet, keys))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public string[] MGet(params string[] keys)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.MGet, keys.JoinToByteArray()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public bool MSet(byte[][] keys, byte[][] values)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.MSet, keys.Merge(values)))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool MSet(string[] keys, string[] values)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.MSet, keys.Merge(values)))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool MSetNx(byte[][] keys, byte[][] values)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.MSetNx, keys.Merge(values)))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool MSetNx(string[] keys, string[] values)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.MSetNx, keys.Merge(values)))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool PSetEx(string key, long milliseconds, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.PSetEx, key.ToBytes(), milliseconds.ToBytes(), value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool Set(string key, byte[] value)
        {
            return Set(key, value, 0, 0);
        }

        public bool Set(string key, byte[] value, int expirySeconds, long expiryMilliseconds = 0)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            if (expirySeconds > 0)
                using (var cmd = new RedisCommand(Db.Db, RedisCommands.Set, key.ToBytes(), value, RedisCommands.Ex, expirySeconds.ToBytes()))
                {
                    return cmd.ExpectSimpleString(Db.Pool, "OK", true);
                }

            if (expiryMilliseconds > 0L)
                using (var cmd = new RedisCommand(Db.Db, RedisCommands.Set, key.ToBytes(), value, RedisCommands.Px, expiryMilliseconds.ToBytes()))
                {
                    return cmd.ExpectSimpleString(Db.Pool, "OK", true);
                }

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.Set, key.ToBytes(), value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool Set(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return Set(key, bytes);
        }

        public bool Set(string key, string value, int expirySeconds, long expiryMilliseconds = 0)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return Set(key, bytes, expirySeconds, expiryMilliseconds);
        }

        public long SetBit(string key, int offset, int value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SetBit, key.ToBytes(), offset.ToBytes(), value.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public bool SetEx(string key, int seconds, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SetEx, key.ToBytes(), seconds.ToBytes(), value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool SetEx(string key, int seconds, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();
            return SetEx(key, seconds, value.ToBytes());
        }

        public bool SetNx(string key, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SetNx, key.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool SetNx(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();
            return SetNx(key, value.ToBytes());
        }

        public long SetRange(string key, int offset, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.SetRange, key.ToBytes(), offset.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long StrLen(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.StrLen, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        #endregion Methods
    }
}
