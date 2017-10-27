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

        public RedisHashesCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInteger HDel(RedisParam key, RedisParam field, params RedisParam[] fields)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields.Length > 0)
            {
                var parameters = key
                                    .Join(field)
                                    .Join(fields);

                return ExpectInteger(RedisCommandList.HDel, parameters);
            }
            return ExpectInteger(RedisCommandList.HDel, key, field);
        }

        public RedisBool HExists(RedisParam key, RedisParam field)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            return ExpectGreaterThanZero(RedisCommandList.HExists, key, field);
        }

        public RedisBytes HGet(RedisParam key, RedisParam field)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            return ExpectBulkStringBytes(RedisCommandList.HGet, key, field);
        }

        public RedisMultiBytes HGetAll(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.HGetAll, key);
        }

        public RedisMultiString HGetAllString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.HGetAll, key);
        }

        public RedisResult<Dictionary<string, string>> HGetAllDictionary(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            var result = ExpectMultiDataStrings(RedisCommandList.HGetAll, key);
            if (result != null)
            {
                var length = result.Length;

                var d = new Dictionary<string, string>(length / 2);
                for (var i = 0; i < length; i += 2)
                    d[result[i]] = result[i + 1];

                return new RedisResult<Dictionary<string, string>>(d);
            }
            return new RedisResult<Dictionary<string, string>>(null);
        }

        public RedisResult<Hashtable> HGetAllHashtable(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            var result = ExpectMultiDataStrings(RedisCommandList.HGetAll, key);
            if (result != null)
            {
                var length = result.Length;

                var h = new Hashtable(length / 2);
                for (var i = 0; i < length; i += 2)
                    h[result[i]] = result[i + 1];

                return new RedisResult<Hashtable>(h);
            }
            return new RedisResult<Hashtable>(null);
        }

        public RedisString HGetString(RedisParam key, RedisParam field)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            return ExpectBulkString(RedisCommandList.HGet, key, field.ToBytes());
        }

        public RedisInteger HIncrBy(RedisParam key, RedisParam field, int increment)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            return ExpectInteger(RedisCommandList.HIncrBy, key, field, increment.ToBytes());
        }

        public RedisInteger HIncrBy(RedisParam key, RedisParam field, long increment)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            return ExpectInteger(RedisCommandList.HIncrBy, key, field, increment.ToBytes());
        }

        public RedisDouble HIncrByFloat(RedisParam key, RedisParam field, double increment)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            return ExpectDouble(RedisCommandList.HIncrByFloat, key, field, increment.ToBytes());
        }

        public RedisMultiBytes HKeys(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.HKeys, key);
        }

        public RedisMultiString HKeyStrings(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.HKeys, key);
        }

        public RedisInteger HLen(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.HLen, key);
        }

        public RedisMultiBytes HMGet(RedisParam key, RedisParam field, params RedisParam[] fields)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields != null && fields.Length > 0)
            {
                var parameters = key
                                    .Join(field)
                                    .Join(fields);

                return ExpectMultiDataBytes(RedisCommandList.HMGet, parameters);
            }
            return ExpectMultiDataBytes(RedisCommandList.HMGet, key, field);
        }

        public RedisMultiString HMGetStrings(RedisParam key, RedisParam field, params RedisParam[] fields)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (fields.Length > 0)
            {
                var parameters = key
                                    .Join(field)
                                    .Join(fields);

                return ExpectMultiDataStrings(RedisCommandList.HMGet, parameters);
            }
            return ExpectMultiDataStrings(RedisCommandList.HMGet, key, field.ToBytes());
        }

        public RedisBool HMSet(RedisParam key, RedisParam field, RedisParam value, RedisParam[] fields = null, RedisParam[] values = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            if (fields.Length > 0)
            {
                if (values == null || values.Length != fields.Length)
                    throw new ArgumentException("Field and values length does not match", "field");

                var parameters = key
                                    .Join(field)
                                    .Join(value)
                                    .Join(fields.Merge(values));

                return ExpectOK(RedisCommandList.HMSet, parameters);
            }
            return ExpectOK(RedisCommandList.HMSet, key, field, value);
        }

        public RedisBool HMSet(RedisParam key, Hashtable values)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (values.IsEmpty())
                throw new ArgumentNullException("values");

            ValidateNotDisposed();

            var parameters = new byte[1 + (2 * values.Count)][];
            parameters[0] = key;

            var i = 1;
            foreach (DictionaryEntry de in values)
            {
                parameters[i++] = de.Key.ToBytes();
                parameters[i++] = de.Value.ToBytes();
            }
            return ExpectOK(RedisCommandList.HMSet, parameters);
        }

        public RedisBool HMSet(RedisParam key, IDictionary<string, string> values)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (values == null || values.Count == 0)
                throw new ArgumentNullException("values");

            ValidateNotDisposed();

            var parameters = new byte[1 + (2 * values.Count)][];
            parameters[0] = key;

            var i = 1;
            foreach (var kvp in values)
            {
                parameters[i++] = kvp.Key.ToBytes();
                parameters[i++] = kvp.Value.ToBytes();
            }
            return ExpectOK(RedisCommandList.HMSet, parameters);
        }

        public RedisBool HMSet(RedisParam key, IDictionary<RedisParam, RedisParam> values)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (values == null || values.Count == 0)
                throw new ArgumentNullException("values");

            ValidateNotDisposed();

            var parameters = new byte[1 + (2 * values.Count)][];
            parameters[0] = key;

            var i = 1;
            foreach (var kvp in values)
            {
                parameters[i++] = kvp.Key;
                parameters[i++] = kvp.Value;
            }
            return ExpectOK(RedisCommandList.HMSet, parameters);
        }

        public RedisMultiBytes HScan(RedisParam key, int count = 10, RedisParam? match = null)
        {
            throw new NotImplementedException();
        }

        public RedisMultiString HScanString(RedisParam key, int count = 10, RedisParam? match = null)
        {
            throw new NotImplementedException();
        }

        public RedisBool HSet(RedisParam key, RedisParam field, RedisParam value)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            if (value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectGreaterThanZero(RedisCommandList.HSet, key, field, value);
        }

        public RedisBool HSetNx(RedisParam key, RedisParam field, RedisParam value)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            ValidateNotDisposed();

            if (value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectGreaterThanZero(RedisCommandList.HSetNx, key, field, value);
        }

        public RedisInteger HStrLen(RedisParam key, RedisParam field)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (field.IsEmpty)
                throw new ArgumentNullException("field");

            return ExpectInteger(RedisCommandList.HStrLen, key, field);
        }

        public RedisMultiBytes HVals(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.HVals, key);
        }

        public RedisMultiString HValStrings(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.HVals, key);
        }

        #endregion Methods
    }
}
