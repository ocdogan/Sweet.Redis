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
    public class RedisVoidResponse : IRedisResponse, IDisposable
    {
        #region .Ctors

        internal RedisVoidResponse()
        { }

        #endregion .Ctors

        #region Destructors

        public void Dispose()
        { }

        #endregion Destructors

        #region Properties

        public virtual int ChildCount { get { return 0; } }

        public virtual byte[] Data { get { return null; } }

        public virtual bool HasChild { get { return false; } }

        public virtual bool HasData { get { return false; } }

        public virtual bool IsVoid { get { return true; } }

        public virtual IList<IRedisResponse> Items { get { return null; } }

        public virtual int Length { get { return 0; } }

        public virtual IRedisResponse Parent { get { return null; } }

        public virtual bool Ready { get { return true; } }

        public virtual RedisRawObjType Type { get { return RedisRawObjType.Undefined; } }

        public virtual int TypeByte { get { return -1; } }

        #endregion Properties

        #region Methods

        public virtual void Clear()
        { }

        public virtual byte[] ReleaseData()
        {
            return null;
        }

        #region Overrides

        public override bool Equals(object obj)
        {
            return (obj is RedisVoidResponse) ||
                (obj is IRedisResponse && ((IRedisResponse)obj).IsVoid);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return String.Empty;
        }

        #endregion Overrides

        #endregion Methods

        #region Operator Overloads

        public static bool operator ==(RedisVoidResponse a, RedisVoidResponse b)
        {
            return (ReferenceEquals(a, null) && ReferenceEquals(b, null));
        }

        public static bool operator !=(RedisVoidResponse a, RedisVoidResponse b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
