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

        public RedisMultiBytes BLPop(RedisParam key, int timeout)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.BLPop, key, timeout.ToBytes());
        }

        public RedisMultiString BLPopString(RedisParam key, int timeout)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.BLPop, key, timeout.ToBytes());
        }

        public RedisMultiBytes BRPop(RedisParam key, int timeout)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.BRPop, key, timeout.ToBytes());
        }

        public RedisMultiString BRPopString(RedisParam key, int timeout)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.BRPop, key, timeout.ToBytes());
        }

        public RedisBytes BRPopLPush(RedisParam source, RedisParam destination)
        {
            if (source.IsNull)
                throw new ArgumentNullException("source");

            if (destination.IsNull)
                throw new ArgumentNullException("destination");

            return ExpectBulkStringBytes(RedisCommands.BRPopLPush, source.ToBytes(), destination.ToBytes());
        }

        public RedisString BRPopLPushString(RedisParam source, RedisParam destination)
        {
            if (source.IsNull)
                throw new ArgumentNullException("source");

            if (destination.IsNull)
                throw new ArgumentNullException("destination");

            return ExpectBulkString(RedisCommands.BRPopLPush, source.ToBytes(), destination.ToBytes());
        }

        public RedisBytes LIndex(RedisParam key, int index)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.LIndex, key, index.ToBytes());
        }

        public RedisString LIndexString(RedisParam key, int index)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.LIndex, key, index.ToBytes());
        }

        public RedisBool LInsert(RedisParam key, bool insertBefore, RedisParam pivot, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            var prePost = insertBefore ? RedisCommands.Before : RedisCommands.After;
            return ExpectOK(RedisCommands.LInsert, key, prePost, pivot, value);
        }

        public RedisInt LLen(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.LLen, key);
        }

        public RedisBytes LPop(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.LPop, key);
        }

        public RedisString LPopString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.LPop, key);
        }

        public RedisInt LPush(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.LPush, key, value);
        }

        public RedisInt LPushX(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.LPushX, key, value);
        }

        public RedisMultiBytes LRange(RedisParam key, int start, int end)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.LRange, key, start.ToBytes(), end.ToBytes());
        }

        public RedisMultiString LRangeString(RedisParam key, int start, int end)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.LRange, key, start.ToBytes(), end.ToBytes());
        }

        public RedisInt LRem(RedisParam key, int count, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.LRem, key, count.ToBytes(), value);
        }

        public RedisBool LSet(RedisParam key, int index, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectOK(RedisCommands.LSet, key, index.ToBytes(), value);
        }

        public RedisBool LTrim(RedisParam key, int start, int end)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectOK(RedisCommands.LTrim, key, start.ToBytes(), end.ToBytes());
        }

        public RedisBytes RPop(RedisParam key)
        {
            var result = RPopString(key);
            if (result != null)
                return Encoding.UTF8.GetBytes(result);
            return new RedisBytes(null);
        }

        public RedisBytes RPopLPush(RedisParam source, RedisParam destination)
        {
            var result = RPopLPushString(source, destination);
            if (result != null)
                return Encoding.UTF8.GetBytes(result);
            return new RedisBytes(null);
        }

        public RedisString RPopLPushString(RedisParam source, RedisParam destination)
        {
            if (source.IsNull)
                throw new ArgumentNullException("source");

            if (destination.IsNull)
                throw new ArgumentNullException("destination");

            return ExpectBulkString(RedisCommands.RPopLPush, source.ToBytes(), destination.ToBytes());
        }

        public RedisString RPopString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.RPop, key);
        }

        public RedisInt RPush(RedisParam key, RedisParam[] values)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (values == null)
                throw new ArgumentNullException("values");

            return ExpectInteger(RedisCommands.RPush, key.Join(values));
        }

        public RedisInt RPushX(RedisParam key, RedisParam value)
        {
            ValidateKeyAndValue(key, value);

            return ExpectInteger(RedisCommands.RPushX, key, value);
        }

        #endregion Methods
    }
}
