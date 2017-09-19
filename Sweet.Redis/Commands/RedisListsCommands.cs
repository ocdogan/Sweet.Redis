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
    internal class RedisListsCommands : RedisCommandSet, IRedisListsCommands
    {
        #region .Ctors

        public RedisListsCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisMultiBytes BLPop(string key, int timeout)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.BLPop, key.ToBytes(), timeout.ToBytes());
        }

        public RedisMultiBytes BRPop(string key, int timeout)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.BRPop, key.ToBytes(), timeout.ToBytes());
        }

        public RedisBytes BRPopLPush(string source, string destination)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (destination == null)
                throw new ArgumentNullException("destination");

            return ExpectBulkStringBytes(RedisCommands.BRPopLPush, source.ToBytes(), destination.ToBytes());
        }

        public RedisString BRPopLPushString(string source, string destination)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (destination == null)
                throw new ArgumentNullException("destination");

            return ExpectBulkString(RedisCommands.BRPopLPush, source.ToBytes(), destination.ToBytes());
        }

        public RedisBytes LIndex(string key, int index)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.LIndex, key.ToBytes(), index.ToBytes());
        }

        public RedisString LIndexString(string key, int index)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.LIndex, key.ToBytes(), index.ToBytes());
        }

        public RedisBool LInsert(string key, bool insertBefore, byte[] pivot, byte[] value)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, value);

            var prePost = insertBefore ? RedisCommands.Before : RedisCommands.After;
            return ExpectOK(RedisCommands.LInsert, key.ToBytes(), prePost, pivot, value);
        }

        public RedisInt LLen(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.LLen, key.ToBytes());
        }

        public RedisBytes LPop(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.LPop, key.ToBytes());
        }

        public RedisString LPopString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.LPop, key.ToBytes());
        }

        public RedisInt LPush(string key, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.LPush, key.ToBytes(), value);
        }

        public RedisInt LPush(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectInteger(RedisCommands.LPush, key.ToBytes(), bytes);
        }

        public RedisInt LPushX(string key, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.LPushX, key.ToBytes(), value);
        }

        public RedisInt LPushX(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectInteger(RedisCommands.LPushX, key.ToBytes(), bytes);
        }

        public RedisMultiBytes LRange(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.LRange, key.ToBytes(), start.ToBytes(), end.ToBytes());
        }

        public RedisMultiString LRangeString(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.LRange, key.ToBytes(), start.ToBytes(), end.ToBytes());
        }

        public RedisInt LRem(string key, int count, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.LRem, key.ToBytes(), count.ToBytes(), value);
        }

        public RedisInt LRem(string key, int count, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectInteger(RedisCommands.LRem, key.ToBytes(), count.ToBytes(), bytes);
        }

        public RedisBool LSet(string key, int index, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.LSet, key.ToBytes(), index.ToBytes(), value);
        }

        public RedisBool LSet(string key, int index, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectOK(RedisCommands.LSet, key.ToBytes(), index.ToBytes(), bytes);
        }

        public RedisBool LTrim(string key, int start, int end)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectOK(RedisCommands.LTrim, key.ToBytes(), start.ToBytes(), end.ToBytes());
        }

        public RedisBytes RPop(string key)
        {
            var result = RPopString(key);
            if (result != null)
                return Encoding.UTF8.GetBytes(result);
            return new RedisBytes(null);
        }

        public RedisBytes RPopLPush(string source, string destination)
        {
            var result = RPopLPushString(source, destination);
            if (result != null)
                return Encoding.UTF8.GetBytes(result);
            return new RedisBytes(null);
        }

        public RedisString RPopLPushString(string source, string destination)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (destination == null)
                throw new ArgumentNullException("destination");

            return ExpectBulkString(RedisCommands.RPopLPush, source.ToBytes(), destination.ToBytes());
        }

        public RedisString RPopString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.RPop, key.ToBytes());
        }

        public RedisInt RPush(string key, byte[][] values)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (values == null)
                throw new ArgumentNullException("values");

            return ExpectInteger(RedisCommands.RPush, key.ToBytes().Join(values));
        }

        public RedisInt RPush(string key, string[] values)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (values == null)
                throw new ArgumentNullException("values");

            return ExpectInteger(RedisCommands.RPush, key.ToBytes().Join(values));
        }

        public RedisInt RPushX(string key, byte[] value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.RPushX, key.ToBytes(), value);
        }

        public RedisInt RPushX(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectInteger(RedisCommands.RPushX, key.ToBytes(), bytes);
        }

        #endregion Methods
    }
}
