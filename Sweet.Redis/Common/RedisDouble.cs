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
    public class RedisDouble : RedisResult<double>
    {
        #region .Ctors

        internal RedisDouble()
        { }

        internal RedisDouble(double value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Double; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var rObj = obj as RedisDouble;
            if (!ReferenceEquals(rObj, null))
                return (rObj.m_Status == m_Status) && Object.Equals(rObj.m_Value, m_Value);
            return false;
        }

        public override int GetHashCode()
        {
            var value = m_Value;
            if (ReferenceEquals(value, null))
                return base.GetHashCode();
            return value.GetHashCode();
        }

        public override string ToString()
        {
            var value = m_Value;
            if (ReferenceEquals(value, null))
                return "(nil)";

            return "\"" + value.ToString() + "\"";
        }
        #endregion Methods

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator RedisDouble(double value)  // implicit double to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator double(RedisDouble value)  // implicit RedisDouble to double conversion operator
        {
            return value.Value;
        }

        public static implicit operator RedisDouble(long value)  // implicit long to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(int value)  // implicit int to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(RedisInt value)  // implicit int to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisDouble a, RedisDouble b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && Object.Equals(a.m_Value, b.m_Value);
        }

        public static bool operator !=(RedisDouble a, RedisDouble b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
