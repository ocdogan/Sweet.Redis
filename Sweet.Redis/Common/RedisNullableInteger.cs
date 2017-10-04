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
    public class RedisNullableInteger : RedisResult<long?>
    {
        #region .Ctors

        internal RedisNullableInteger()
        { }

        internal RedisNullableInteger(long? value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.NullableInteger; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var rObj = obj as RedisNullableInteger;
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
            var value = (long?)m_RawData;
            if (value == null)
                return "(nil)";

            return ":" + value.Value.ToString();
        }

        #endregion Methods

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator RedisNullableInteger(long? value)  // implicit long to RedisNullableInt conversion operator
        {
            return new RedisNullableInteger(value);
        }

        public static implicit operator RedisNullableInteger(int? value)  // implicit int to RedisNullableInt conversion operator
        {
            return new RedisNullableInteger(value);
        }

        public static implicit operator RedisNullableInteger(double? value)  // implicit double to RedisNullableInt conversion operator
        {
            return new RedisNullableInteger((long)value);
        }

        public static implicit operator RedisNullableInteger(RedisDouble value)  // implicit RedisDouble to RedisNullableInt conversion operator
        {
            return new RedisNullableInteger((long?)value.Value);
        }

        public static implicit operator RedisNullableInteger(RedisNullableDouble value)  // implicit RedisNullableDouble to RedisNullableInt conversion operator
        {
            return new RedisNullableInteger((long?)value.Value);
        }

        public static implicit operator long? (RedisNullableInteger value)  // implicit RedisNullableInt to long conversion operator
        {
            return value.Value;
        }

        public static implicit operator int? (RedisNullableInteger value)  // implicit RedisNullableInt to int conversion operator
        {
            return (int)value.Value;
        }

        public static implicit operator double? (RedisNullableInteger value)  // implicit RedisNullableInt to double conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisNullableInteger a, RedisNullableInteger b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && (a.m_RawData == b.m_RawData);
        }

        public static bool operator !=(RedisNullableInteger a, RedisNullableInteger b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
