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

        public RedisDouble(double value)
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

            var dObj = obj as RedisDouble;
            if (!ReferenceEquals(dObj, null))
                return (dObj.m_Status == m_Status) && ((double)m_RawData).Equals((double)dObj.m_RawData);

            if (obj is double)
            {
                var d = (double)obj;
                return (m_Status == RedisResultStatus.Completed) && ((double)m_RawData).Equals(d);
            }

            if (obj is long)
            {
                var l = (long)obj;
                return (m_Status == RedisResultStatus.Completed) && ((double)m_RawData).Equals((double)l);
            }

            if (obj is int)
            {
                var i = (int)obj;
                return (m_Status == RedisResultStatus.Completed) && ((double)m_RawData).Equals((double)i);
            }

            if (obj is DateTime)
            {
                var l = ((DateTime)obj).Ticks;
                return (m_Status == RedisResultStatus.Completed) && ((double)m_RawData).Equals((double)l);
            }

            var rObj = obj as RedisResult<double>;
            if (!ReferenceEquals(rObj, null))
                return (rObj.Status == m_Status) && ((double)m_RawData).Equals((double)rObj.RawData);

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
            if (ReferenceEquals(value, null))
                return "(nil)";

            return "\"" + value + "\"";
        }
        #endregion Methods

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator double(RedisDouble value)  // implicit RedisDouble to double conversion operator
        {
            return value.Value;
        }

        public static implicit operator RedisDouble(double value)  // implicit double to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(long value)  // implicit long to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(int value)  // implicit int to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(short value)  // implicit short to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(decimal value)  // implicit decimal to RedisDouble conversion operator
        {
            return new RedisDouble(Convert.ToDouble(value));
        }

        public static implicit operator RedisDouble(float value)  // implicit float to RedisDouble conversion operator
        {
            return new RedisDouble(Convert.ToDouble(value));
        }

        public static implicit operator RedisDouble(ulong value)  // implicit ulong to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(uint value)  // implicit uint to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(ushort value)  // implicit ushort to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        public static implicit operator RedisDouble(RedisInteger value)  // implicit int to RedisDouble conversion operator
        {
            return new RedisDouble(value);
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(double a, RedisDouble b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && a.Equals((double)b.m_RawData);
        }

        public static bool operator !=(double a, RedisDouble b)
        {
            return !(b == a); 
        }

        public static bool operator ==(RedisDouble a, double b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && b.Equals((double)a.m_RawData);
        }

        public static bool operator !=(RedisDouble a, double b)
        {
            return !(a == b); 
        }

        public static bool operator ==(short a, RedisDouble b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((double)b.m_RawData).Equals((double)a);
        }

        public static bool operator !=(short a, RedisDouble b)
        {
            return !(b == a); 
        }

        public static bool operator ==(RedisDouble a, short b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && ((double)a.m_RawData).Equals((double)b);
        }

        public static bool operator !=(RedisDouble a, short b)
        {
            return !(a == b); 
        }

        public static bool operator ==(int a, RedisDouble b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((double)b.m_RawData).Equals((double)a);
        }

        public static bool operator !=(int a, RedisDouble b)
        {
            return !(b == a); 
        }

        public static bool operator ==(RedisDouble a, int b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && ((double)a.m_RawData).Equals((double)b);
        }

        public static bool operator !=(RedisDouble a, int b)
        {
            return !(a == b); 
        }

        public static bool operator ==(long a, RedisDouble b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((double)b.m_RawData).Equals((double)a);
        }

        public static bool operator !=(long a, RedisDouble b)
        {
            return !(b == a); 
        }

        public static bool operator ==(RedisDouble a, long b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && !((double)a.m_RawData).Equals((double)b);
        }

        public static bool operator !=(RedisDouble a, long b)
        {
            return !(a == b); 
        }

        public static bool operator ==(RedisDouble a, RedisDouble b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && ((double)a.m_RawData).Equals((double)b.m_RawData);
        }

        public static bool operator !=(RedisDouble a, RedisDouble b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
