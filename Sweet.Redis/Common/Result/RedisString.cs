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
    public class RedisString : RedisResult<string, char>
    {
        #region .Ctors

        internal RedisString()
        { }

        public RedisString(string value)
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

        public override int Length
        {
            get
            {
                ValidateCompleted();
                var val = Value;
                return (val != null) ? val.Length : 0;
            }
        }

        public override RedisResultType Type { get { return RedisResultType.String; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return ReferenceEquals(m_RawData, null);

            if (ReferenceEquals(obj, this))
                return true;

            var sObj = obj as RedisString;
            if (!ReferenceEquals(sObj, null))
                return (sObj.m_Status == m_Status) && ((string)sObj.m_RawData == (string)m_RawData);

            var str = obj as string;
            if (!ReferenceEquals(str, null))
                return ((string)m_RawData == str);

            var eObj = obj as RedisError;
            if (!ReferenceEquals(eObj, null))
                return (eObj.Status == m_Status) && ((string)eObj.RawData == (string)m_RawData);

            var rObj = obj as RedisResult<string>;
            if (!ReferenceEquals(rObj, null))
                return (rObj.Status == m_Status) && ((string)rObj.RawData == (string)m_RawData);

            var bytes = obj as byte[];
            if (!ReferenceEquals(bytes, null))
                return bytes.EqualTo(((string)m_RawData).ToBytes());

            var bObj = obj as RedisBytes;
            if (!ReferenceEquals(bObj, null))
                return (bObj.Status == m_Status) && ((byte[])bObj.RawData).EqualTo(((string)m_RawData).ToBytes());

            var rbObj = obj as RedisResult<byte[]>;
            if (!ReferenceEquals(rbObj, null))
                return (rbObj.Status == m_Status) && ((byte[])rbObj.RawData).EqualTo(((string)m_RawData).ToBytes());

            return false;
        }

        public override int GetHashCode()
        {
            var value = m_RawData;
            if (ReferenceEquals(value, null))
                return base.GetHashCode();
            return value.GetHashCode();
        }

        public override string ToString()
        {
            var value = (string)m_RawData;
            if (ReferenceEquals(value, null))
                return "(nil)";

            if (value.Length == 0)
                return "(empty)";

            return "\"" + value + "\"";
        }

        #endregion Methods

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator RedisString(string value)  // implicit string to RedisString conversion operator
        {
            return new RedisString(value);
        }

        public static implicit operator string(RedisString value)  // implicit RedisString to string conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisString a, string b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            return (a.m_Status == RedisResultStatus.Completed) && ((string)a.m_RawData == b);
        }

        public static bool operator !=(RedisString a, string b)
        {
            return !(a == b);
        }

        public static bool operator ==(string a, RedisString b)
        {
            return (b == a);
        }

        public static bool operator !=(string a, RedisString b)
        {
            return !(b == a);
        }

        public static bool operator ==(RedisString a, RedisString b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && ((string)a.m_RawData == (string)b.m_RawData);
        }

        public static bool operator !=(RedisString a, RedisString b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
