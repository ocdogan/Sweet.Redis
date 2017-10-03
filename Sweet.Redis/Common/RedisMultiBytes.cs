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
    public class RedisMultiBytes : RedisResult<byte[][], byte[]>
    {
        #region .Ctors

        internal RedisMultiBytes()
        { }

        internal RedisMultiBytes(byte[][] value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override byte[] this[int index]
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

        public override RedisResultType Type { get { return RedisResultType.MultiBytes; } }

        #endregion Properties

        #region Conversion Methods

        public static implicit operator RedisMultiBytes(byte[][] value)  // implicit byte[][] to RedisMultiBytes conversion operator
        {
            return new RedisMultiBytes(value);
        }

        public static implicit operator byte[][] (RedisMultiBytes value)  // implicit RedisMultiBytes to byte[][] conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var rObj = obj as RedisMultiBytes;
            if (!ReferenceEquals(rObj, null))
                return (rObj.m_Status == m_Status) && (rObj.m_RawData == m_RawData);
            return false;
        }

        public override int GetHashCode()
        {
            var val = m_RawData;
            if (ReferenceEquals(val, null))
                return base.GetHashCode();
            return val.GetHashCode();
        }

        public override string ToString()
        {
            var value = m_RawData;
            if (value == null)
                return "(nil)";

            var multiBytes = value as byte[][];
            if (multiBytes == null)
                return "(nil)";

            var length = multiBytes.Length;
            if (length == 0)
                return "(empty)";

            var sBuilder = new StringBuilder();

            for (var i = 0; i < length; i++)
            {
                sBuilder.Append(i + 1);
                sBuilder.Append(") ");

                var bytes = multiBytes[i];
                if (bytes == null)
                    sBuilder.Append("(nil)");
                else if (bytes.Length == 0)
                    sBuilder.Append("(empty)");
                else
                {
                    sBuilder.Append('"');
                    sBuilder.Append(Encoding.UTF8.GetString(bytes));
                    sBuilder.Append('"');
                }

                sBuilder.AppendLine();
            }

            return sBuilder.ToString();
        }

        #endregion Overrides

        #endregion Methods

        #region Operator Overloads

        public static bool operator ==(RedisMultiBytes a, RedisMultiBytes b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && (a.m_RawData == b.m_RawData);
        }

        public static bool operator !=(RedisMultiBytes a, RedisMultiBytes b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
