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

        public RedisInt Append(string key, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.Append, key.ToBytes(), value);
        }

        public RedisInt BitCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.BitCount, key.ToBytes());
        }

        public RedisInt BitCount(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.BitCount, key.ToBytes(), start.ToBytes(), end.ToBytes());
        }

        public RedisInt Decr(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Decr, key.ToBytes());
        }

        public RedisInt DecrBy(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.DecrBy, key.ToBytes(), count.ToBytes());
        }

        public RedisInt DecrBy(string key, long count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.DecrBy, key.ToBytes(), count.ToBytes());
        }

        public RedisBytes Get(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Get, key.ToBytes());
        }

        public RedisInt GetBit(string key, int offset)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.GetBit, key.ToBytes(), offset.ToBytes());
        }

        public RedisBytes GetRange(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.GetRange, key.ToBytes(), start.ToBytes(), end.ToBytes());
        }

        public RedisString GetRangeString(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.GetRange, key.ToBytes(), start.ToBytes(), end.ToBytes());
        }

        public RedisBytes GetSet(string key, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            var result = ExpectBulkString(RedisCommands.GetSet, key.ToBytes(), value);
            if (result != null)
                return Encoding.UTF8.GetBytes(result);
            return null;
        }

        public RedisString GetSet(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectBulkString(RedisCommands.GetSet, key.ToBytes(), bytes);
        }

        public RedisString GetString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.Get, key.ToBytes());
        }

        public RedisInt Incr(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Incr, key.ToBytes());
        }

        public RedisInt IncrBy(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.IncrBy, key.ToBytes(), count.ToBytes());
        }

        public RedisInt IncrBy(string key, long count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.IncrBy, key.ToBytes(), count.ToBytes());
        }

        public RedisDouble IncrByFloat(string key, double increment)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectDouble(RedisCommands.IncrBy, key.ToBytes(), increment.ToBytes());
        }

        public RedisMultiBytes MGet(params byte[][] keys)
        {
            return ExpectMultiDataBytes(RedisCommands.MGet, keys);
        }

        public RedisMultiString MGet(params string[] keys)
        {
            return ExpectMultiDataStrings(RedisCommands.MGet, keys.ConvertToByteArray());
        }

        public RedisBool MSet(byte[][] keys, byte[][] values)
        {
            return ExpectOK(RedisCommands.MSet, keys.Merge(values));
        }

        public RedisBool MSet(string[] keys, string[] values)
        {
            return ExpectOK(RedisCommands.MSet, keys.Merge(values));
        }

        public RedisBool MSetNx(byte[][] keys, byte[][] values)
        {
            return ExpectGreaterThanZero(RedisCommands.MSetNx, keys.Merge(values));
        }

        public RedisBool MSetNx(string[] keys, string[] values)
        {
            return ExpectGreaterThanZero(RedisCommands.MSetNx, keys.Merge(values));
        }

        public RedisBool PSetEx(string key, long milliseconds, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.PSetEx, key.ToBytes(), milliseconds.ToBytes(), value);
        }

        public RedisBool Set(string key, byte[] value)
        {
            return Set(key, value, 0, 0);
        }

        public RedisBool Set(string key, byte[] value, int expirySeconds, long expiryMilliseconds = 0)
        {
            ValidateKeyAndValue(key, value);

            if (expirySeconds > 0)
                return ExpectOK(RedisCommands.Set, key.ToBytes(), value, RedisCommands.EX, expirySeconds.ToBytes());

            if (expiryMilliseconds > RedisConstants.Zero)
                return ExpectOK(RedisCommands.Set, key.ToBytes(), value, RedisCommands.PX, expiryMilliseconds.ToBytes());

            return ExpectOK(RedisCommands.Set, key.ToBytes(), value);
        }

        public RedisBool Set(string key, string value)
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

        public RedisBool Set(string key, string value, int expirySeconds, long expiryMilliseconds = 0)
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

        public RedisInt SetBit(string key, int offset, int value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.SetBit, key.ToBytes(), offset.ToBytes(), value.ToBytes());
        }

        public RedisBool SetEx(string key, int seconds, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.SetEx, key.ToBytes(), seconds.ToBytes(), value);
        }

        public RedisBool SetEx(string key, int seconds, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            return SetEx(key, seconds, value.ToBytes());
        }

        public RedisBool SetNx(string key, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectGreaterThanZero(RedisCommands.SetNx, key.ToBytes(), value);
        }

        public RedisBool SetNx(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            return SetNx(key, value.ToBytes());
        }

        public RedisInt SetRange(string key, int offset, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.SetRange, key.ToBytes(), offset.ToBytes(), value);
        }

        public RedisInt StrLen(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.StrLen, key.ToBytes());
        }

        #endregion Methods
    }
}
