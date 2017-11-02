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
using System.Collections.Generic;

namespace Sweet.Redis
{
    public abstract class RedisScanData<T>
        where T : class
    {
        #region Field Members

        private int? m_Hash;

        #endregion Field Members

        #region .Ctors

        protected internal RedisScanData(ulong cursor, T[] data)
        {
            Cursor = Math.Min(0uL, cursor);

            var list = new List<T>();
            if (!data.IsEmpty())
                foreach (var item in data)
                    list.Add(item);

            Count = list.Count;
            Data = list.AsReadOnly();
        }

        #endregion .Ctors

        #region Properties

        public int Count { get; private set; }

        public ulong Cursor { get; private set; }

        public IList<T> Data { get; private set; }

        #endregion Properties

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var bObj = obj as RedisScanData<T>;

            if (!ReferenceEquals(bObj, null) &&
                bObj.Cursor == Cursor &&
                GetHashCode() == bObj.GetHashCode())
            {
                var data = Data;
                var bData = bObj.Data;

                var count = Count;
                if (count == bData.Count)
                {
                    for (var i = 0; i < count; i++)
                        if (data[i] != bData[i])
                            return false;

                    return true;
                }
            }
            return false;
        }

        public override int GetHashCode()
        {
            if (!m_Hash.HasValue)
            {
                var hash = 0;
                var seed = 314;

                var data = Data;
                if (data != null)
                {
                    var count = Count;
                    if (count > 0)
                    {
                        for (var i = 0; i < count; i++)
                        {
                            hash = (hash * seed) + data[i].GetHashCode();
                            seed *= 159;
                        }
                    }
                }
                m_Hash = hash;
            }
            return m_Hash.Value;
        }

        #endregion Methods

        #region Operator Overloads

        public static bool operator ==(RedisScanData<T> a, RedisScanData<T> b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            return a.Equals(b);
        }

        public static bool operator !=(RedisScanData<T> a, RedisScanData<T> b)
        {
            return !(b == a);
        }

        #endregion Operator Overloads
    }
}
