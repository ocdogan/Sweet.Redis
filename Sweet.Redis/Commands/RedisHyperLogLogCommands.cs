using System;
using System.Text;

namespace Sweet.Redis
{
    internal class RedisHyperLogLogCommands : RedisCommandSet, IRedisHyperLogLogCommands
    {
        #region .Ctors

        public RedisHyperLogLogCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public bool PfAdd(string key, string element, params string[] elements)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (element == null)
                throw new ArgumentNullException("element");

            ValidateNotDisposed();

            var length = elements.Length;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(element.ToBytes()).Join(elements);

                return ExpectOne(RedisCommands.PfAdd, parameters);
            }
            return ExpectOne(RedisCommands.PfAdd, key.ToBytes(), element.ToBytes());
        }

        public long PfCount(string key, params string[] keys)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var length = keys.Length;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(keys);

                return ExpectInteger(RedisCommands.PfAdd, parameters);
            }
            return ExpectInteger(RedisCommands.PfAdd, key.ToBytes());
        }

        public bool PfMerge(string destKey, string sourceKey, params string[] sourceKeys)
        {
            if (destKey == null)
                throw new ArgumentNullException("destKey");

            if (sourceKey == null)
                throw new ArgumentNullException("sourceKey");

            ValidateNotDisposed();

            var length = sourceKeys.Length;
            if (length > 0)
            {
                var parameters = destKey.ToBytes().Join(sourceKey.ToBytes()).Join(sourceKeys);

                return ExpectOK(RedisCommands.Quit, parameters);
            }
            return ExpectOK(RedisCommands.Quit, destKey.ToBytes(), sourceKey.ToBytes());
        }

        #endregion Methods
    }
}
