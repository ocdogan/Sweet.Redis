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

namespace Sweet.Redis
{
    public class RedisScanBytes : RedisResult<RedisScanBytesData>
    {
        #region .Ctors

        internal RedisScanBytes()
        { }

        public RedisScanBytes(RedisScanBytesData value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Scan; } }

        #endregion Properties

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var bObj = obj as RedisScanBytes;
            if (!ReferenceEquals(bObj, null))
                return (bObj.m_Status == m_Status) && (bObj.m_RawData == m_RawData);

            var rObj = obj as RedisResult<RedisScanBytes>;
            if (!ReferenceEquals(rObj, null))
                return (rObj.Status == m_Status) && (rObj.RawData == m_RawData);

            return false;
        }

        public override int GetHashCode()
        {
            var value = m_RawData;
            if (ReferenceEquals(value, null))
                return base.GetHashCode();
            return value.GetHashCode();
        }

        #endregion Methods

        #region Conversion Methods

        public static implicit operator RedisScanBytes(RedisScanBytesData value)  // implicit RedisScan conversion operator
        {
            return new RedisScanBytes(value);
        }

        public static implicit operator RedisScanBytesData(RedisScanBytes value)  // implicit RedisScan conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisScanBytes a, RedisScanBytes b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            return a.Equals(b);
        }

        public static bool operator !=(RedisScanBytes a, RedisScanBytes b)
        {
            return !(b == a);
        }

        #endregion Operator Overloads
    }
}
