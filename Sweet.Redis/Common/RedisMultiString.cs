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
    public class RedisMultiString : RedisResult<string[], string>
    {
        #region .Ctors

        internal RedisMultiString()
        { }

        internal RedisMultiString(string[] value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override string this[int index]
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

        public override RedisResultType Type { get { return RedisResultType.MultiString; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var rObj = obj as RedisMultiString;
            if (!ReferenceEquals(rObj, null))
                return (rObj.m_Status == m_Status) && (rObj.m_RawData == m_RawData);
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
            var value = m_RawData;
            if (value == null)
                return "(nil)";

            var strings = value as string[];
            if (strings == null)
                return "(nil)";

            var length = strings.Length;
            if (length == 0)
                return "(empty)";

            var sBuilder = new StringBuilder();

            for (var i = 0; i < length; i++)
            {
                sBuilder.Append(i + 1);
                sBuilder.Append(") ");

                var s = strings[i];
                if (s == null)
                    sBuilder.Append("(nil)");
                else if (s.Length == 0)
                    sBuilder.Append("(empty)");
                else
                {
                    sBuilder.Append('"');
                    sBuilder.Append(s);
                    sBuilder.Append('"');
                }

                sBuilder.AppendLine();
            }

            return sBuilder.ToString();
        }

        #endregion Overrides

        #endregion Methods

        #region Conversion Methods

        public static implicit operator RedisMultiString(string[] value)  // implicit string[] to RedisMultiString conversion operator
        {
            return new RedisMultiString(value);
        }

        public static implicit operator string[] (RedisMultiString value)  // implicit RedisMultiString to string[] conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisMultiString a, RedisMultiString b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && (a.m_RawData == b.m_RawData);
        }

        public static bool operator !=(RedisMultiString a, RedisMultiString b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
