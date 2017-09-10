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
