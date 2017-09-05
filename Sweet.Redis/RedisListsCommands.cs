using System;
using System.Text;

namespace Sweet.Redis
{
    internal class RedisListsCommands : RedisCommandSet, IRedisListsCommands
    {
        #region .Ctors

        public RedisListsCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public byte[][] BLPop(string key, int timeout)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.BLPop, key.ToBytes(), timeout.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public byte[][] BRPop(string key, int timeout)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.BRPop, key.ToBytes(), timeout.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public byte[] BRPopLPush(string source, string destination)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (destination == null)
                throw new ArgumentNullException("destination");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.BRPopLPush, source.ToBytes(), destination.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public string BRPopLPushString(string source, string destination)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (destination == null)
                throw new ArgumentNullException("destination");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.BRPopLPush, source.ToBytes(), destination.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public byte[] LIndex(string key, int index)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LIndex, key.ToBytes(), index.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public string LIndexString(string key, int index)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LIndex, key.ToBytes(), index.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public bool LInsert(string key, bool insertBefore, byte[] pivot, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            var prePost = insertBefore ? RedisCommands.Before : RedisCommands.After;
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LInsert, key.ToBytes(), prePost, pivot, value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public long LLen(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LLen, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[] LPop(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LPop, key.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public string LPopString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LPop, key.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public long LPush(string key, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LPush, key.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long LPush(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LPush, key.ToBytes(), bytes))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long LPushX(string key, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LPushX, key.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long LPushX(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LPushX, key.ToBytes(), bytes))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[][] LRange(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LRange, key.ToBytes(), start.ToBytes(), end.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public string[] LRangeString(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LRange, key.ToBytes(), start.ToBytes(), end.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public long LRem(string key, int count, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LRem, key.ToBytes(), count.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long LRem(string key, int count, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LRem, key.ToBytes(), count.ToBytes(), bytes))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public bool LSet(string key, int index, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LSet, key.ToBytes(), index.ToBytes(), value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool LSet(string key, int index, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LSet, key.ToBytes(), index.ToBytes(), bytes))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool LTrim(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.LTrim, key.ToBytes(), start.ToBytes(), end.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public byte[] RPop(string key)
        {
            var result = RPopString(key);
            if (result != null)
                return Encoding.UTF8.GetBytes(result);
            return null;
        }

        public byte[] RPopLPush(string source, string destination)
        {
            var result = RPopLPushString(source, destination);
            if (result != null)
                return Encoding.UTF8.GetBytes(result);
            return null;
        }

        public string RPopLPushString(string source, string destination)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (destination == null)
                throw new ArgumentNullException("destination");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RPopLPush, source.ToBytes(), destination.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public string RPopString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RPop, key.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public long RPush(string key, byte[][] values)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (values == null)
                throw new ArgumentNullException("values");

            ValidateNotDisposed();

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RPush, key.ToBytes().Merge(values)))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long RPush(string key, string[] values)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (values == null)
                throw new ArgumentNullException("values");

            ValidateNotDisposed();

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RPush, key.ToBytes().Merge(values)))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long RPushX(string key, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RPushX, key.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long RPushX(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(Db.Db, RedisCommands.RPushX, key.ToBytes(), bytes))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        #endregion Methods
    }
}
