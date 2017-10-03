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
    public class RedisVoidResponse : RedisRawResponse, IDisposable
    {
        #region Static Members

        public static readonly RedisVoidResponse Void = new RedisVoidResponse();

        #endregion Static Members

        #region .Ctors

        internal RedisVoidResponse()
        { }

        #endregion .Ctors

        #region Destructors

        public override void Dispose()
        { }

        protected override void OnDispose(bool disposing)
        { }

        #endregion Destructors

        #region Properties

        public override int ChildCount { get { return 0; } }

        public override byte[] Data { get { return null; } }

        public override bool HasChild { get { return false; } }

        public override bool HasData { get { return false; } }

        public override bool IsVoid { get { return true; } }

        public override IList<IRedisRawResponse> Items { get { return null; } }

        public override int Length { get { return 0; } }

        public override IRedisRawResponse Parent { get { return null; } }

        public override bool Ready { get { return true; } }

        public override RedisRawObjType Type { get { return RedisRawObjType.Undefined; } }

        public override int TypeByte { get { return -1; } }

        #endregion Properties

        #region Methods

        public override void Clear()
        { }

        public override byte[] ReleaseData()
        {
            return null;
        }

        #region Overrides

        public override bool Equals(object obj)
        {
            return (obj is RedisVoidResponse) ||
                (obj is IRedisRawResponse && ((IRedisRawResponse)obj).IsVoid);
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
