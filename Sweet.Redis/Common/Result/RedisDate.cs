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
    public class RedisDate : RedisResult<DateTime>
    {
        #region .Ctors

        internal RedisDate()
        { }

        public RedisDate(DateTime value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Boolean; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var dObj = obj as RedisDate;
            if (!ReferenceEquals(dObj, null))
                return (dObj.m_Status == m_Status) && ((DateTime)dObj.m_RawData == (DateTime)m_RawData);

            if (obj is DateTime)
            {
                var date = (DateTime)obj;
                return (m_Status == RedisResultStatus.Completed) && (date == (DateTime)m_RawData);
            }

            if (obj is long)
            {
                var date = new DateTime((long)obj);
                return (m_Status == RedisResultStatus.Completed) && (date == (DateTime)m_RawData);
            }

            if (obj is double)
            {
                var date = new DateTime((long)(double)obj);
                return (m_Status == RedisResultStatus.Completed) && (date == (DateTime)m_RawData);
            }

            var rObj = obj as RedisResult<DateTime>;
            if (!ReferenceEquals(rObj, null))
                return (rObj.Status == m_Status) && ((DateTime)rObj.RawData == (DateTime)m_RawData);

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

        public static implicit operator RedisDate(DateTime value)  // implicit DateTime to RedisDate conversion operator
        {
            return new RedisDate(value);
        }

        public static implicit operator DateTime(RedisDate value)  // implicit RedisDate to DateTime conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(DateTime a, RedisDate b)
        {
            if (ReferenceEquals(a, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((DateTime)b.m_RawData == a);
        }

        public static bool operator !=(DateTime a, RedisDate b)
        {
            return b != a;
        }

        public static bool operator ==(RedisDate a, DateTime b)
        {
            if (ReferenceEquals(a, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && ((DateTime)a.m_RawData == b);
        }

        public static bool operator !=(RedisDate a, DateTime b)
        {
            return !(a == b);
        }

        public static bool operator ==(RedisDate a, RedisDate b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && ((DateTime)a.m_RawData == (DateTime)b.m_RawData);
        }

        public static bool operator !=(RedisDate a, RedisDate b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
