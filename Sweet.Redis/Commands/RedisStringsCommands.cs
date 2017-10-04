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

        public RedisStringsCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInteger Append(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.Append, key, value);
        }

        public RedisInteger BitCount(RedisParam key)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.BitCount, key);
        }

        public RedisInteger BitCount(RedisParam key, int start, int end)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.BitCount, key, start.ToBytes(), end.ToBytes());
        }

        public RedisInteger Decr(RedisParam key)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Decr, key);
        }

        public RedisInteger DecrBy(RedisParam key, int count)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.DecrBy, key, count.ToBytes());
        }

        public RedisInteger DecrBy(RedisParam key, long count)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.DecrBy, key, count.ToBytes());
        }

        public RedisBytes Get(RedisParam key)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.Get, key);
        }

        public RedisInteger GetBit(RedisParam key, int offset)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.GetBit, key, offset.ToBytes());
        }

        public RedisBytes GetRange(RedisParam key, int start, int end)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.GetRange, key, start.ToBytes(), end.ToBytes());
        }

        public RedisString GetRangeString(RedisParam key, int start, int end)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.GetRange, key, start.ToBytes(), end.ToBytes());
        }

        public RedisBytes GetSet(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);
            return ExpectBulkStringBytes(RedisCommands.GetSet, key, value);
        }

        public RedisString GetSetString(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);
            return ExpectBulkString(RedisCommands.GetSet, key, value);
        }

        public RedisString GetString(RedisParam key)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.Get, key);
        }

        public RedisInteger Incr(RedisParam key)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.Incr, key);
        }

        public RedisInteger IncrBy(RedisParam key, int count)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.IncrBy, key, count.ToBytes());
        }

        public RedisInteger IncrBy(RedisParam key, long count)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.IncrBy, key, count.ToBytes());
        }

        public RedisDouble IncrByFloat(RedisParam key, double increment)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectDouble(RedisCommands.IncrBy, key, increment.ToBytes());
        }

        public RedisMultiBytes MGet(params RedisParam[] keys)
        {
            return ExpectMultiDataBytes(RedisCommands.MGet, keys.ConvertToByteArray());
        }

        public RedisMultiString MGetString(params RedisParam[] keys)
        {
            return ExpectMultiDataStrings(RedisCommands.MGet, keys.ConvertToByteArray());
        }

        public RedisBool MSet(RedisParam[] keys, RedisParam[] values)
        {
            return ExpectOK(RedisCommands.MSet, keys.Merge(values));
        }

        public RedisBool MSetNx(RedisParam[] keys, RedisParam[] values)
        {
            return ExpectGreaterThanZero(RedisCommands.MSetNx, keys.Merge(values));
        }

        public RedisBool PSetEx(RedisParam key, long milliseconds, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.PSetEx, key, milliseconds.ToBytes(), value);
        }

        public RedisBool Set(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);
            return ExpectOK(RedisCommands.Set, key, value);
        }

        public RedisBool Set(RedisParam key, RedisParam value, int expirySeconds, long expiryMilliseconds = 0)
        {
            ValidateKeyAndValue(key, value);

            if (expirySeconds > 0)
                return ExpectOK(RedisCommands.Set, key, value, RedisCommands.EX, expirySeconds.ToBytes());

            if (expiryMilliseconds > RedisConstants.Zero)
                return ExpectOK(RedisCommands.Set, key, value, RedisCommands.PX, expiryMilliseconds.ToBytes());

            return ExpectOK(RedisCommands.Set, key, value);
        }

        public RedisInteger SetBit(RedisParam key, int offset, int value)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.SetBit, key, offset.ToBytes(), value.ToBytes());
        }

        public RedisBool SetEx(RedisParam key, int seconds, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.SetEx, key, seconds.ToBytes(), value);
        }

        public RedisBool SetEx(RedisParam key, int seconds, string value)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            return SetEx(key, seconds, value.ToBytes());
        }

        public RedisBool SetNx(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);
            return ExpectGreaterThanZero(RedisCommands.SetNx, key, value);
        }

        public RedisInteger SetRange(RedisParam key, int offset, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.SetRange, key, offset.ToBytes(), value);
        }

        public RedisInteger StrLen(RedisParam key)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.StrLen, key);
        }

        #endregion Methods
    }
}
