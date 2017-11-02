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

        public RedisSetsCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInteger SAdd(RedisParam key, RedisParam member, params RedisParam[] members)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            if (members.Length > 0)
            {
                var parameters = key
                                    .Join(member)
                                    .Join(members);

                return ExpectInteger(RedisCommandList.SAdd, parameters);
            }
            return ExpectInteger(RedisCommandList.SAdd, key, member);
        }

        public RedisInteger SCard(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.SCard, key);
        }

        public RedisMultiBytes SDiff(RedisParam fromKey, params RedisParam[] keys)
        {
            if (fromKey.IsNull)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = fromKey.Join(keys);
                return ExpectMultiDataBytes(RedisCommandList.SDiff, parameters);
            }
            return ExpectMultiDataBytes(RedisCommandList.SDiff, fromKey);
        }

        public RedisInteger SDiffStore(RedisParam toKey, RedisParam fromKey, params RedisParam[] keys)
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

                return ExpectInteger(RedisCommandList.SDiffStore, parameters);
            }
            return ExpectInteger(RedisCommandList.SDiffStore, toKey, fromKey);
        }

        public RedisMultiString SDiffString(RedisParam fromKey, params RedisParam[] keys)
        {
            if (fromKey.IsNull)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = fromKey.Join(keys);
                return ExpectMultiDataStrings(RedisCommandList.SDiff, parameters);
            }
            return ExpectMultiDataStrings(RedisCommandList.SDiff, fromKey);
        }

        public RedisMultiBytes SInter(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataBytes(RedisCommandList.SInter, parameters);
            }
            return ExpectMultiDataBytes(RedisCommandList.SDiff, key);
        }

        public RedisInteger SInterStore(RedisParam toKey, params RedisParam[] keys)
        {
            if (toKey.IsNull)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey.Join(keys);
                return ExpectInteger(RedisCommandList.SInterStore, parameters);
            }
            return ExpectInteger(RedisCommandList.SInterStore, toKey);
        }

        public RedisMultiString SInterStrings(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataStrings(RedisCommandList.SInter, parameters);
            }
            return ExpectMultiDataStrings(RedisCommandList.SDiff, key);
        }

        public RedisBool SIsMember(RedisParam key, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommandList.SIsMember, key, member);
        }

        public RedisMultiBytes SMembers(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.SMembers, key);
        }

        public RedisMultiString SMemberStrings(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.SMembers, key);
        }

        public RedisBool SMove(RedisParam fromKey, RedisParam toKey, RedisParam member)
        {
            if (fromKey.IsNull)
                throw new ArgumentNullException("fromKey");

            if (toKey.IsNull)
                throw new ArgumentNullException("toKey");

            return ExpectGreaterThanZero(RedisCommandList.SMove, fromKey, toKey, member);
        }

        public RedisBytes SPop(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommandList.SPop, key);
        }

        public RedisString SPopString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommandList.SPop, key);
        }

        public RedisBytes SRandMember(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommandList.SRandMember, key);
        }

        public RedisMultiBytes SRandMember(RedisParam key, int count)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.SRandMember, key, count.ToBytes());
        }

        public RedisString SRandMemberString(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommandList.SRandMember, key);
        }

        public RedisMultiString SRandMemberString(RedisParam key, int count)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.SRandMember, key, count.ToBytes());
        }

        public RedisInteger SRem(RedisParam key, RedisParam member, params RedisParam[] members)
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

                return ExpectInteger(RedisCommandList.SRem, parameters);
            }
            return ExpectInteger(RedisCommandList.SRem, key, member);
        }

        public RedisScanBytes SScan(RedisParam key, ulong cursor = 0uL, int count = 10, RedisParam? match = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = new byte[][] { key.Data, cursor.ToBytes() };

            if (match.HasValue)
            {
                var value = match.Value;
                if (!value.IsEmpty)
                {
                    parameters = parameters.Join(RedisCommandList.Match);
                    parameters = parameters.Join(value.Data);
                }
            }

            if (count > 0)
            {
                parameters = parameters.Join(RedisCommandList.Count);
                parameters = parameters.Join(count.ToBytes());
            }

            return RedisCommandUtils.ToScanBytes(ExpectArray(RedisCommandList.SScan, parameters));
        }

        public RedisScanStrings SScanString(RedisParam key, ulong cursor = 0uL, int count = 10, RedisParam? match = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = new byte[][] { key.Data, cursor.ToBytes() };

            if (match.HasValue)
            {
                var value = match.Value;
                if (!value.IsEmpty)
                {
                    parameters = parameters.Join(RedisCommandList.Match);
                    parameters = parameters.Join(value.Data);
                }
            }

            if (count > 0)
            {
                parameters = parameters.Join(RedisCommandList.Count);
                parameters = parameters.Join(count.ToBytes());
            }

            return RedisCommandUtils.ToScanStrings(ExpectArray(RedisCommandList.SScan, parameters));
        }

        public RedisMultiBytes SUnion(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataBytes(RedisCommandList.SUnion, parameters);
            }
            return ExpectMultiDataBytes(RedisCommandList.SUnion, key);
        }

        public RedisInteger SUnionStore(RedisParam toKey, params RedisParam[] keys)
        {
            if (toKey.IsNull)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey.Join(keys);
                return ExpectInteger(RedisCommandList.SUnionStore, parameters);
            }
            return ExpectInteger(RedisCommandList.SUnionStore, toKey);
        }

        public RedisMultiString SUnionStrings(RedisParam key, params RedisParam[] keys)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.Join(keys);
                return ExpectMultiDataStrings(RedisCommandList.SUnion, parameters);
            }
            return ExpectMultiDataStrings(RedisCommandList.SUnion, key);
        }

        #endregion Methods
    }
}
