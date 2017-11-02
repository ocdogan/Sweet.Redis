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
    public class RedisInteger : RedisResult<long>
    {
        #region .Ctors

        internal RedisInteger()
        { }

        public RedisInteger(long value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Integer; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var dObj = obj as RedisInteger;
            if (!ReferenceEquals(dObj, null))
                return (dObj.m_Status == m_Status) && ((long)m_RawData).Equals((long)dObj.m_RawData);

            if (obj is double)
            {
                var d = (double)obj;
                return (m_Status == RedisResultStatus.Completed) && ((long)m_RawData).Equals(d);
            }

            if (obj is long)
            {
                var l = (long)obj;
                return (m_Status == RedisResultStatus.Completed) && ((long)m_RawData == l);
            }

            if (obj is int)
            {
                var i = (int)obj;
                return (m_Status == RedisResultStatus.Completed) && ((long)m_RawData == i);
            }

            if (obj is DateTime)
            {
                var l = ((DateTime)obj).Ticks;
                return (m_Status == RedisResultStatus.Completed) && ((long)m_RawData == l);
            }

            var rObj = obj as RedisResult<long>;
            if (!ReferenceEquals(rObj, null))
                return (rObj.Status == m_Status) && ((long)m_RawData == (long)rObj.RawData);

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

            return ":" + value;
        }

        #endregion Overrides

        #endregion Methods

        #region Conversion Methods

        public static implicit operator RedisInteger(long value)  // implicit long to RedisInt conversion operator
        {
            return new RedisInteger(value);
        }

        public static implicit operator RedisInteger(int value)  // implicit int to RedisInt conversion operator
        {
            return new RedisInteger(value);
        }

        public static implicit operator RedisInteger(short value)  // implicit short to RedisInt conversion operator
        {
            return new RedisInteger(value);
        }

        public static implicit operator RedisInteger(double value)  // implicit double to RedisInt conversion operator
        {
            return new RedisInteger((long)value);
        }

        public static implicit operator RedisInteger(decimal value)  // implicit decimal to RedisInt conversion operator
        {
            return new RedisInteger(Convert.ToInt64(value));
        }

        public static implicit operator RedisInteger(float value)  // implicit float to RedisInt conversion operator
        {
            return new RedisInteger(Convert.ToInt64(value));
        }

        public static implicit operator RedisInteger(ulong value)  // implicit ulong to RedisInt conversion operator
        {
            return new RedisInteger((long)value);
        }

        public static implicit operator RedisInteger(uint value)  // implicit uint to RedisInt conversion operator
        {
            return new RedisInteger(value);
        }

        public static implicit operator RedisInteger(ushort value)  // implicit ushort to RedisInt conversion operator
        {
            return new RedisInteger(value);
        }

        public static implicit operator RedisInteger(RedisDouble value)  // implicit RedisDouble to RedisInt conversion operator
        {
            return new RedisInteger((long)value.Value);
        }

        public static implicit operator long(RedisInteger value)  // implicit RedisInt to long conversion operator
        {
            return value.Value;
        }

        public static implicit operator int(RedisInteger value)  // implicit RedisInt to int conversion operator
        {
            return (int)value.Value;
        }

        public static implicit operator short(RedisInteger value)  // implicit RedisInt to short conversion operator
        {
            return (short)value.Value;
        }

        public static implicit operator double(RedisInteger value)  // implicit RedisInt to double conversion operator
        {
            return value.Value;
        }

        public static implicit operator decimal(RedisInteger value)  // implicit RedisInt to decimal conversion operator
        {
            return value.Value;
        }

        public static implicit operator float(RedisInteger value)  // implicit RedisInt to float conversion operator
        {
            return value.Value;
        }

        public static implicit operator ulong(RedisInteger value)  // implicit RedisInt to ulong conversion operator
        {
            return (ulong)value.Value;
        }

        public static implicit operator uint(RedisInteger value)  // implicit RedisInt to uint conversion operator
        {
            return (uint)value.Value;
        }

        public static implicit operator ushort(RedisInteger value)  // implicit RedisInt to ushort conversion operator
        {
            return (ushort)value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(double a, RedisInteger b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && a.Equals((double)(long)b.m_RawData);
        }

        public static bool operator !=(double a, RedisInteger b)
        {
            return !(b == a);
        }

        public static bool operator ==(RedisInteger a, double b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && b.Equals((double)(long)a.m_RawData);
        }

        public static bool operator !=(RedisInteger a, double b)
        {
            return !(a == b); 
        }

        public static bool operator ==(short a, RedisInteger b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((long)b.m_RawData == a);
        }

        public static bool operator !=(short a, RedisInteger b)
        {
            return !(b == a); 
        }

        public static bool operator ==(RedisInteger a, short b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && ((long)a.m_RawData == b);
        }

        public static bool operator !=(RedisInteger a, short b)
        {
            return !(a == b); 
        }

        public static bool operator ==(int a, RedisInteger b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((long)b.m_RawData == a);
        }

        public static bool operator !=(int a, RedisInteger b)
        {
            return !(b == a); 
        }

        public static bool operator ==(RedisInteger a, int b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && ((long)a.m_RawData == b);
        }

        public static bool operator !=(RedisInteger a, int b)
        {
            return !(a == b); 
        }

        public static bool operator ==(long a, RedisInteger b)
        {
            if (ReferenceEquals(b, null) || ReferenceEquals(b.m_RawData, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((long)b.m_RawData == a);
        }

        public static bool operator !=(long a, RedisInteger b)
        {
            return !(b == a); 
        }

        public static bool operator ==(RedisInteger a, long b)
        {
            if (ReferenceEquals(a, null) || ReferenceEquals(a.m_RawData, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && ((long)a.m_RawData == b);
        }

        public static bool operator !=(RedisInteger a, long b)
        {
            return !(a == b); 
        }

        public static bool operator ==(RedisInteger a, RedisInteger b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && ((long)a.m_RawData == (long)b.m_RawData);
        }

        public static bool operator !=(RedisInteger a, RedisInteger b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
