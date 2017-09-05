using System;
using System.Collections;
using System.Collections.Generic;

namespace Sweet.Redis
{
    internal class RedisHashesCommands : RedisCommandSet, IRedisHashesCommands
    {
        #region .Ctors

        public RedisHashesCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public long HDel(string key, byte[] field, params byte[][] fields)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields != null && fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(field)
                                    .Merge(fields);

                using (var cmd = new RedisCommand(RedisCommands.HDel, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HDel, key.ToBytes(), field))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long HDel(string key, string field, params string[] fields)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields != null && fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(field.ToBytes())
                                    .Merge(fields.ToBytesArray());

                using (var cmd = new RedisCommand(RedisCommands.HDel, parameters))
                {
                    return cmd.ExpectInteger(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HDel, key.ToBytes(), field.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public bool HExists(string key, byte[] field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HExists, key.ToBytes(), field))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool HExists(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HExists, key.ToBytes(), field.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public byte[] HGet(string key, byte[] field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HGet, key.ToBytes(), field))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public byte[] HGet(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HGet, key.ToBytes(), field.ToBytes()))
            {
                return cmd.ExpectBulkStringBytes(Db.Pool, true);
            }
        }

        public byte[][] HGetAll(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HGetAll, key.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public Dictionary<string, string> HGetAllDictionary(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HGetAll, key.ToBytes()))
            {
                var result = cmd.ExpectMultiDataStrings(Db.Pool, true);
                if (result != null)
                {
                    var length = result.Length;

                    var d = new Dictionary<string, string>(length / 2);
                    for (var i = 0; i < length; i += 2)
                        d[result[i]] = result[i + 1];

                    return d;
                }
                return null;
            }
        }

        public Hashtable HGetAllHashtable(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HGetAll, key.ToBytes()))
            {
                var result = cmd.ExpectMultiDataStrings(Db.Pool, true);
                if (result != null)
                {
                    var length = result.Length;

                    var h = new Hashtable(length / 2);
                    for (var i = 0; i < length; i += 2)
                        h[result[i]] = result[i + 1];

                    return h;
                }
                return null;
            }
        }

        public string HGetString(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HGet, key.ToBytes(), field.ToBytes()))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public long HIncrBy(string key, byte[] field, int increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HIncrBy, key.ToBytes(), field, increment.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long HIncrBy(string key, byte[] field, long increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HIncrBy, key.ToBytes(), field, increment.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public double HIncrByFloat(string key, byte[] field, double increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HIncrByFloat, key.ToBytes(), field, increment.ToBytes()))
            {
                return cmd.ExpectDouble(Db.Pool, true);
            }
        }

        public byte[][] HKeys(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HKeys, key.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public string[] HKeyStrings(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HKeys, key.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public long HLen(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HLen, key.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[][] HMGet(string key, byte[] field, params byte[][] fields)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields != null && fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(field)
                                    .Merge(fields);

                using (var cmd = new RedisCommand(RedisCommands.HMGet, parameters))
                {
                    return cmd.ExpectMultiDataBytes(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HMGet, key.ToBytes(), field))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public byte[][] HMGet(string key, string field, params string[] fields)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields != null && fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(field.ToBytes())
                                    .Merge(fields.ToBytesArray());

                using (var cmd = new RedisCommand(RedisCommands.HMGet, parameters))
                {
                    return cmd.ExpectMultiDataBytes(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HMGet, key.ToBytes(), field.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public string[] HMGetStrings(string key, string field, params string[] fields)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields != null && fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Merge(field.ToBytes())
                                    .Merge(fields.ToBytesArray());

                using (var cmd = new RedisCommand(RedisCommands.HMGet, parameters))
                {
                    return cmd.ExpectMultiDataStrings(Db.Pool, true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HMGet, key.ToBytes(), field.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public bool HMSet(string key, byte[] field, byte[] value, byte[][] fields = null, byte[][] values = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (value != null && value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            if (fields != null && fields.Length > 0)
            {
                if (values == null || values.Length != fields.Length)
                    throw new ArgumentException("Field and values length does not match", "field");

                var parameters = key.ToBytes()
                                    .Merge(field)
                                    .Merge(value)
                                    .Merge(fields.Merge(values));

                using (var cmd = new RedisCommand(RedisCommands.HMSet, parameters))
                {
                    return cmd.ExpectSimpleString(Db.Pool, "OK", true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HMSet, key.ToBytes(), field, value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool HMSet(string key, string field, byte[] value, string[] fields = null, byte[][] values = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (value != null && value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            if (fields != null && fields.Length > 0)
            {
                if (values == null || values.Length != fields.Length)
                    throw new ArgumentException("Field and values length does not match", "field");

                var parameters = key.ToBytes()
                                    .Merge(field.ToBytes())
                                    .Merge(value)
                                    .Merge(fields.ToBytesArray().Merge(values));

                using (var cmd = new RedisCommand(RedisCommands.HMSet, parameters))
                {
                    return cmd.ExpectSimpleString(Db.Pool, "OK", true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HMSet, key.ToBytes(), field.ToBytes(), value))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool HMSet(string key, string field, string value, string[] fields = null, string[] values = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            if (fields != null && fields.Length > 0)
            {
                if (values == null || values.Length != fields.Length)
                    throw new ArgumentException("Field and values length does not match", "field");

                var parameters = key.ToBytes()
                                    .Merge(field.ToBytes())
                                    .Merge(bytes)
                                    .Merge(fields.ToBytesArray().Merge(values.ToBytesArray()));

                using (var cmd = new RedisCommand(RedisCommands.HMSet, parameters))
                {
                    return cmd.ExpectSimpleString(Db.Pool, "OK", true);
                }
            }
            using (var cmd = new RedisCommand(RedisCommands.HMSet, key.ToBytes(), field.ToBytes(), bytes))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool HMSet(string key, Hashtable values)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (values == null || values.Count == 0)
                throw new ArgumentNullException("values");

            ValidateNotDisposed();

            var parameters = new byte[1 + (2 * values.Count)][];
            parameters[0] = key.ToBytes();

            var i = 1;
            foreach (DictionaryEntry de in values)
            {
                parameters[i++] = de.Key.ToBytes();
                parameters[i++] = de.Value.ToBytes();
            }
            using (var cmd = new RedisCommand(RedisCommands.HMSet, parameters))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool HMSet(string key, Dictionary<string, string> values)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (values == null || values.Count == 0)
                throw new ArgumentNullException("values");

            ValidateNotDisposed();

            var parameters = new byte[1 + (2 * values.Count)][];
            parameters[0] = key.ToBytes();

            var i = 1;
            foreach (var kvp in values)
            {
                parameters[i++] = kvp.Key.ToBytes();
                parameters[i++] = kvp.Value.ToBytes();
            }
            using (var cmd = new RedisCommand(RedisCommands.HMSet, parameters))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public byte[][] HScan(string key, int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public string[] HScanString(string key, int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public bool HSet(string key, byte[] field, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            if (value != null && value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HSet, key.ToBytes(), field, value))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool HSet(string key, string field, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            if (value != null && value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HSet, key.ToBytes(), field.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool HSet(string key, string field, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(RedisCommands.HSet, key.ToBytes(), field.ToBytes(), bytes))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool HSetNx(string key, byte[] field, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (value != null && value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(RedisCommands.HSetNx, key.ToBytes(), field, value))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool HSetNx(string key, string field, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (value != null && value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(RedisCommands.HSetNx, key.ToBytes(), field.ToBytes(), value))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public bool HSetNx(string key, string field, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(RedisCommands.HSetNx, key.ToBytes(), field.ToBytes(), bytes))
            {
                return cmd.ExpectInteger(Db.Pool, true) > 0;
            }
        }

        public long HStrLen(string key, byte[] field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HStrLen, key.ToBytes(), field))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public long HStrLen(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HStrLen, key.ToBytes(), field.ToBytes()))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public byte[][] HVals(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HVals, key.ToBytes()))
            {
                return cmd.ExpectMultiDataBytes(Db.Pool, true);
            }
        }

        public string[] HValStrings(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.HVals, key.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        #endregion Methods
    }
}
