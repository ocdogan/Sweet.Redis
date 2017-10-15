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
    public class RedisError : RedisResult<string, char>
    {
        #region .Ctors

        internal RedisError()
        { }

        public RedisError(string value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override char this[int index]
        {
            get
            {
                ValidateCompleted();
                if (index < 0)
                    throw new ArgumentOutOfRangeException("index", "Index value is out of range");

                var val = Value;
                if (val != null)
                    return val[index];

                throw new ArgumentOutOfRangeException("index", "Index value is out of range");
            }
        }

        public override RedisResultType Type { get { return RedisResultType.Error; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return ReferenceEquals(m_RawData, null);

            if (ReferenceEquals(obj, this))
                return true;

            var eObj = obj as RedisError;
            if (!ReferenceEquals(eObj, null))
                return (eObj.m_Status == m_Status) && ((string)eObj.m_RawData == (string)m_RawData);

            var str = obj as string;
            if (!ReferenceEquals(str, null))
                return ((string)m_RawData == str);

            var sObj = obj as RedisString;
            if (!ReferenceEquals(sObj, null))
                return (sObj.Status == m_Status) && ((string)sObj.RawData == (string)m_RawData);

            var rObj = obj as RedisResult<string>;
            if (!ReferenceEquals(rObj, null))
                return (rObj.Status == m_Status) && ((string)rObj.RawData == (string)m_RawData);

            var bytes = obj as byte[];
            if (!ReferenceEquals(bytes, null))
                return (Encoding.UTF8.GetBytes((string)m_RawData) == bytes);

            var bObj = obj as RedisBytes;
            if (!ReferenceEquals(bObj, null))
                return (bObj.Status == m_Status) && ((byte[])bObj.RawData == Encoding.UTF8.GetBytes((string)m_RawData));

            var rbObj = obj as RedisResult<byte[]>;
            if (!ReferenceEquals(rbObj, null))
                return (rbObj.Status == m_Status) && ((byte[])rbObj.RawData == Encoding.UTF8.GetBytes((string)m_RawData));

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

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator RedisError(string value)  // implicit string to RedisError conversion operator
        {
            return new RedisError(value);
        }

        public static implicit operator string(RedisError value)  // implicit RedisError to string conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(string a, RedisError b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            return (b.m_Status == RedisResultStatus.Completed) && ((string)b.m_RawData == a);
        }

        public static bool operator !=(string a, RedisError b)
        {
            return b != a;
        }

        public static bool operator ==(RedisError a, string b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            return (a.m_Status == RedisResultStatus.Completed) && ((string)a.m_RawData == b);
        }

        public static bool operator !=(RedisError a, string b)
        {
            return !(a == b);
        }

        public static bool operator ==(RedisError a, RedisError b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && ((string)a.m_RawData == (string)b.m_RawData);
        }

        public static bool operator !=(RedisError a, RedisError b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
