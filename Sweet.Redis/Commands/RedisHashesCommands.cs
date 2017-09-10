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

            if (fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(field)
                                    .Join(fields);

                return ExpectInteger(RedisCommands.HDel, parameters);
            }
            return ExpectInteger(RedisCommands.HDel, key.ToBytes(), field);
        }

        public long HDel(string key, string field, params string[] fields)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(field.ToBytes())
                                    .Join(fields.ToBytesArray());

                return ExpectInteger(RedisCommands.HDel, parameters);
            }
            return ExpectInteger(RedisCommands.HDel, key.ToBytes(), field.ToBytes());
        }

        public bool HExists(string key, byte[] field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectGreaterThanZero(RedisCommands.HExists, key.ToBytes(), field);
        }

        public bool HExists(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectGreaterThanZero(RedisCommands.HExists, key.ToBytes(), field.ToBytes());
        }

        public byte[] HGet(string key, byte[] field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectBulkStringBytes(RedisCommands.HGet, key.ToBytes(), field);
        }

        public byte[] HGet(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectBulkStringBytes(RedisCommands.HGet, key.ToBytes(), field.ToBytes());
        }

        public byte[][] HGetAll(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.HGetAll, key.ToBytes());
        }

        public Dictionary<string, string> HGetAllDictionary(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            var result = ExpectMultiDataStrings(RedisCommands.HGetAll, key.ToBytes());
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

        public Hashtable HGetAllHashtable(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            var result = ExpectMultiDataStrings(RedisCommands.HGetAll, key.ToBytes());
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

        public string HGetString(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectBulkString(RedisCommands.HGet, key.ToBytes(), field.ToBytes());
        }

        public long HIncrBy(string key, byte[] field, int increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectInteger(RedisCommands.HIncrBy, key.ToBytes(), field, increment.ToBytes());
        }

        public long HIncrBy(string key, byte[] field, long increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectInteger(RedisCommands.HIncrBy, key.ToBytes(), field, increment.ToBytes());
        }

        public double HIncrByFloat(string key, byte[] field, double increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectDouble(RedisCommands.HIncrByFloat, key.ToBytes(), field, increment.ToBytes());
        }

        public byte[][] HKeys(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.HKeys, key.ToBytes());
        }

        public string[] HKeyStrings(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.HKeys, key.ToBytes());
        }

        public long HLen(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.HLen, key.ToBytes());
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
                                    .Join(field)
                                    .Join(fields);

                return ExpectMultiDataBytes(RedisCommands.HMGet, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.HMGet, key.ToBytes(), field);
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
                                    .Join(field.ToBytes())
                                    .Join(fields.ToBytesArray());

                return ExpectMultiDataBytes(RedisCommands.HMGet, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.HMGet, key.ToBytes(), field.ToBytes());
        }

        public string[] HMGetStrings(string key, string field, params string[] fields)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(field.ToBytes())
                                    .Join(fields.ToBytesArray());

                return ExpectMultiDataStrings(RedisCommands.HMGet, parameters);
            }
            return ExpectMultiDataStrings(RedisCommands.HMGet, key.ToBytes(), field.ToBytes());
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

            if (fields.Length > 0)
            {
                if (values == null || values.Length != fields.Length)
                    throw new ArgumentException("Field and values length does not match", "field");

                var parameters = key.ToBytes()
                                    .Join(field)
                                    .Join(value)
                                    .Join(fields.Merge(values));

                return ExpectOK(RedisCommands.HMSet, parameters);
            }
            return ExpectOK(RedisCommands.HMSet, key.ToBytes(), field, value);
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

            if (fields.Length > 0)
            {
                if (values == null || values.Length != fields.Length)
                    throw new ArgumentException("Field and values length does not match", "field");

                var parameters = key.ToBytes()
                                    .Join(field.ToBytes())
                                    .Join(value)
                                    .Join(fields.Merge(values));

                return ExpectOK(RedisCommands.HMSet, parameters);
            }
            return ExpectOK(RedisCommands.HMSet, key.ToBytes(), field.ToBytes(), value);
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

            if (fields.Length > 0)
            {
                if (values == null || values.Length != fields.Length)
                    throw new ArgumentException("Field and values length does not match", "field");

                var parameters = key.ToBytes()
                                    .Join(field.ToBytes())
                                    .Join(bytes)
                                    .Join(fields.Merge(values));

                return ExpectOK(RedisCommands.HMSet, parameters);
            }
            return ExpectOK(RedisCommands.HMSet, key.ToBytes(), field.ToBytes(), bytes);
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
            return ExpectOK(RedisCommands.HMSet, parameters);
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
            return ExpectOK(RedisCommands.HMSet, parameters);
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

            return ExpectGreaterThanZero(RedisCommands.HSet, key.ToBytes(), field, value);
        }

        public bool HSet(string key, string field, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            if (value != null && value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectGreaterThanZero(RedisCommands.HSet, key.ToBytes(), field.ToBytes(), value);
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

            return ExpectGreaterThanZero(RedisCommands.HSet, key.ToBytes(), field.ToBytes(), bytes);
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

            return ExpectGreaterThanZero(RedisCommands.HSetNx, key.ToBytes(), field, value);
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

            return ExpectGreaterThanZero(RedisCommands.HSetNx, key.ToBytes(), field.ToBytes(), value);
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

            return ExpectGreaterThanZero(RedisCommands.HSetNx, key.ToBytes(), field.ToBytes(), bytes);
        }

        public long HStrLen(string key, byte[] field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectInteger(RedisCommands.HStrLen, key.ToBytes(), field);
        }

        public long HStrLen(string key, string field)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (field == null || field.Length == 0)
                throw new ArgumentNullException("field");

            return ExpectInteger(RedisCommands.HStrLen, key.ToBytes(), field.ToBytes());
        }

        public byte[][] HVals(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.HVals, key.ToBytes());
        }

        public string[] HValStrings(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.HVals, key.ToBytes());
        }

        #endregion Methods
    }
}
