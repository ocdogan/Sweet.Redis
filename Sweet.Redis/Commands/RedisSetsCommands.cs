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

namespace Sweet.Redis
{
    internal class RedisSetsCommands : RedisCommandSet, IRedisSetsCommands
    {
        #region .Ctors

        public RedisSetsCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInt SAdd(RedisParam key, RedisParam member, params RedisParam[] members)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            if (members.Length > 0)
            {
                var parameters = key
                                    .Join(member)
                                    .Join(members);

                return ExpectInteger(RedisCommands.SAdd, parameters);
            }
            return ExpectInteger(RedisCommands.SAdd, key, member);
        }

        public RedisInt SCard(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.SCard, key);
        }

        public RedisMultiBytes SDiff(RedisParam fromKey, params RedisParam[] keys)
        {
            if (fromKey.IsNull)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = fromKey.Join(keys);
                return ExpectMultiDataBytes(RedisCommands.SDiff, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.SDiff, fromKey);
        }

        public RedisInt SDiffStore(RedisParam toKey, RedisParam fromKey, params RedisParam[] keys)
        {
            if (toKey.IsNull)
                throw new ArgumentNullException("toKey");

            if (fromKey.IsNull)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey
                                      .Join(fromKey)
                                      .Join(keys);

                return ExpectInteger(RedisCommands.SDiffStore, parameters);
            }
            return ExpectInteger(RedisCommands.SDiffStore, toKey, fromKey);
        }

        public RedisMultiString SDiffString(RedisParam fromKey, params RedisParam[] keys)
        {
            if (fromKey.IsNull)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = fromKey.Join(keys);
                return ExpectMultiDataStrings(RedisCommands.SDiff, parameters);
            }
            return ExpectMultiDataStrings(RedisCommands.SDiff, fromKey);
        }

        public RedisMultiBytes SInter(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataBytes(RedisCommands.SInter, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.SDiff, key);
        }

        public RedisInt SInterStore(RedisParam toKey, params RedisParam[] keys)
        {
            if (toKey.IsNull)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey.Join(keys);
                return ExpectInteger(RedisCommands.SInterStore, parameters);
            }
            return ExpectInteger(RedisCommands.SInterStore, toKey);
        }

        public RedisMultiString SInterStrings(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataStrings(RedisCommands.SInter, parameters);
            }
            return ExpectMultiDataStrings(RedisCommands.SDiff, key);
        }

        public RedisBool SIsMember(RedisParam key, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.SIsMember, key, member);
        }

        public RedisMultiBytes SMembers(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.SMembers, key);
        }

        public RedisMultiString SMemberStrings(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.SMembers, key);
        }

        public RedisBool SMove(RedisParam fromKey, RedisParam toKey, RedisParam member)
        {
            if (fromKey.IsNull)
                throw new ArgumentNullException("fromKey");

            if (toKey.IsNull)
                throw new ArgumentNullException("toKey");

            return ExpectGreaterThanZero(RedisCommands.SMove, fromKey, toKey, member);
        }

        public RedisBytes SPop(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.SPop, key);
        }

        public RedisString SPopString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.SPop, key);
        }

        public RedisBytes SRandMember(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.SRandMember, key);
        }

        public RedisMultiBytes SRandMember(RedisParam key, int count)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.SRandMember, key, count.ToBytes());
        }

        public RedisString SRandMemberString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.SRandMember, key);
        }

        public RedisMultiString SRandMemberString(RedisParam key, int count)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.SRandMember, key, count.ToBytes());
        }

        public RedisInt SRem(RedisParam key, RedisParam member, params RedisParam[] members)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            if (members.Length > 0)
            {
                var parameters = key
                                      .Join(member)
                                      .Join(members);

                return ExpectInteger(RedisCommands.SRem, parameters);
            }
            return ExpectInteger(RedisCommands.SRem, key, member);
        }

        public RedisMultiBytes SScan(RedisParam key, int count = 10, RedisParam? match = null)
        {
            throw new NotImplementedException();
        }

        public RedisMultiString SScanString(RedisParam key, int count = 10, RedisParam? match = null)
        {
            throw new NotImplementedException();
        }

        public RedisMultiBytes SUnion(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataBytes(RedisCommands.SUnion, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.SUnion, key);
        }

        public RedisInt SUnionStore(RedisParam toKey, params RedisParam[] keys)
        {
            if (toKey.IsNull)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey.Join(keys);
                return ExpectInteger(RedisCommands.SUnionStore, parameters);
            }
            return ExpectInteger(RedisCommands.SUnionStore, toKey);
        }

        public RedisMultiString SUnionStrings(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataStrings(RedisCommands.SUnion, parameters);
            }
            return ExpectMultiDataStrings(RedisCommands.SUnion, key);
        }

        #endregion Methods
    }
}
