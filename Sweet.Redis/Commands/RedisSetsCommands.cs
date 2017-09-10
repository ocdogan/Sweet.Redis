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

        public long SAdd(string key, byte[] member, params byte[][] members)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            if (members.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(member)
                                    .Join(members);

                return ExpectInteger(RedisCommands.SAdd, parameters);
            }
            return ExpectInteger(RedisCommands.SAdd, key.ToBytes(), member);
        }

        public long SAdd(string key, string member, params string[] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (members.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(member.ToBytes())
                                    .Join(members.ToBytesArray());

                return ExpectInteger(RedisCommands.SAdd, parameters);
            }
            return ExpectInteger(RedisCommands.SAdd, key.ToBytes(), member.ToBytes());
        }

        public long SCard(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.SCard, key.ToBytes());
        }

        public byte[][] SDiff(string fromKey, params string[] keys)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = fromKey.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectMultiDataBytes(RedisCommands.SDiff, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.SDiff, fromKey.ToBytes());
        }

        public long SDiffStore(string toKey, string fromKey, params string[] keys)
        {
            if (toKey == null)
                throw new ArgumentNullException("toKey");

            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey.ToBytes()
                                      .Join(fromKey.ToBytes())
                                      .Join(keys.ToBytesArray());

                return ExpectInteger(RedisCommands.SDiffStore, parameters);
            }
            return ExpectInteger(RedisCommands.SDiffStore, toKey.ToBytes(), fromKey.ToBytes());
        }

        public string[] SDiffString(string fromKey, params string[] keys)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = fromKey.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectMultiDataStrings(RedisCommands.SDiff, parameters);
            }
            return ExpectMultiDataStrings(RedisCommands.SDiff, fromKey.ToBytes());
        }

        public byte[][] SInter(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectMultiDataBytes(RedisCommands.SInter, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.SDiff, key.ToBytes());
        }

        public long SInterStore(string toKey, params string[] keys)
        {
            if (toKey == null)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey.ToBytes()
                                      .Join(keys.ToBytesArray());

                return ExpectInteger(RedisCommands.SInterStore, parameters);
            }
            return ExpectInteger(RedisCommands.SInterStore, toKey.ToBytes());
        }

        public string[] SInterStrings(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectMultiDataStrings(RedisCommands.SInter, parameters);
            }
            return ExpectMultiDataStrings(RedisCommands.SDiff, key.ToBytes());
        }

        public bool SIsMember(string key, byte[] member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.SIsMember, key.ToBytes(), member);
        }

        public bool SIsMember(string key, string member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectGreaterThanZero(RedisCommands.SIsMember, key.ToBytes(), member.ToBytes());
        }

        public byte[][] SMembers(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.SMembers, key.ToBytes());
        }

        public string[] SMemberStrings(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.SMembers, key.ToBytes());
        }

        public bool SMove(string fromKey, string toKey, byte[] member)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            if (toKey == null)
                throw new ArgumentNullException("toKey");

            return ExpectGreaterThanZero(RedisCommands.SMove, fromKey.ToBytes(), toKey.ToBytes(), member);
        }

        public bool SMove(string fromKey, string toKey, string member)
        {
            if (fromKey == null)
                throw new ArgumentNullException("fromKey");

            if (toKey == null)
                throw new ArgumentNullException("toKey");

            return ExpectGreaterThanZero(RedisCommands.SMove, fromKey.ToBytes(), toKey.ToBytes(), member.ToBytes());
        }

        public byte[] SPop(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.SPop, key.ToBytes());
        }

        public string SPopString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.SPop, key.ToBytes());
        }

        public byte[] SRandMember(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkStringBytes(RedisCommands.SRandMember, key.ToBytes());
        }

        public byte[][] SRandMember(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommands.SRandMember, key.ToBytes(), count.ToBytes());
        }

        public string SRandMemberString(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectBulkString(RedisCommands.SRandMember, key.ToBytes());
        }

        public string[] SRandMemberString(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommands.SRandMember, key.ToBytes(), count.ToBytes());
        }

        public long SRem(string key, byte[] member, params byte[][] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            if (members.Length > 0)
            {
                var parameters = key.ToBytes()
                                      .Join(member.ToBytes())
                                      .Join(members);

                return ExpectInteger(RedisCommands.SRem, parameters);
            }
            return ExpectInteger(RedisCommands.SRem, key.ToBytes(), member);
        }

        public long SRem(string key, string member, params string[] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            if (members.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(member.ToBytes())
                                    .Join(members.ToBytesArray());

                return ExpectInteger(RedisCommands.SRem, parameters);
            }
            return ExpectInteger(RedisCommands.SRem, key.ToBytes(), member.ToBytes());
        }

        public byte[][] SScan(string key, int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public string[] SScanString(string key, int count = 10, string match = null)
        {
            throw new NotImplementedException();
        }

        public byte[][] SUnion(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectMultiDataBytes(RedisCommands.SUnion, parameters);
            }
            return ExpectMultiDataBytes(RedisCommands.SUnion, key.ToBytes());
        }

        public long SUnionStore(string toKey, params string[] keys)
        {
            if (toKey == null)
                throw new ArgumentNullException("toKey");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = toKey.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectInteger(RedisCommands.SUnionStore, parameters);
            }
            return ExpectInteger(RedisCommands.SUnionStore, toKey.ToBytes());
        }

        public string[] SUnionStrings(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (keys.Length > 0)
            {
                var parameters = key.ToBytes()
                                    .Join(keys.ToBytesArray());

                return ExpectMultiDataStrings(RedisCommands.SUnion, parameters);
            }
            return ExpectMultiDataStrings(RedisCommands.SUnion, key.ToBytes());
        }

        #endregion Methods
    }
}
