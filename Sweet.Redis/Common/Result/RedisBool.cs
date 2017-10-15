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
    public class RedisBool : RedisResult<bool>
    {
        #region .Ctors

        internal RedisBool()
        { }

        public RedisBool(bool value)
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

            var bObj = obj as RedisBool;
            if (!ReferenceEquals(bObj, null))
                return (bObj.m_Status == m_Status) && (bObj.m_RawData == m_RawData);

            var rObj = obj as RedisResult<bool>;
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

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator RedisBool(bool value)  // implicit bool to RedisBool conversion operator
        {
            return new RedisBool(value);
        }

        public static implicit operator RedisBool(byte value)  // implicit byte to RedisBool conversion operator
        {
            return new RedisBool(value == 1);
        }

        public static implicit operator RedisBool(short value)  // implicit short to RedisBool conversion operator
        {
            return new RedisBool(value == 1);
        }

        public static implicit operator RedisBool(int value)  // implicit int to RedisBool conversion operator
        {
            return new RedisBool(value == 1);
        }

        public static implicit operator RedisBool(long value)  // implicit long to RedisBool conversion operator
        {
            return new RedisBool(value == 1L);
        }

        public static implicit operator RedisBool(double value)  // implicit double to RedisBool conversion operator
        {
            return new RedisBool(value == 1d);
        }

        public static implicit operator RedisBool(decimal value)  // implicit decimal to RedisBool conversion operator
        {
            return new RedisBool(value == 1m);
        }

        public static implicit operator RedisBool(float value)  // implicit float to RedisBool conversion operator
        {
            return new RedisBool(value == 1f);
        }

        public static implicit operator RedisBool(ushort value)  // implicit ushort to RedisBool conversion operator
        {
            return new RedisBool(value == 1u);
        }

        public static implicit operator RedisBool(uint value)  // implicit uint to RedisBool conversion operator
        {
            return new RedisBool(value == 1u);
        }

        public static implicit operator RedisBool(ulong value)  // implicit uint to RedisBool conversion operator
        {
            return new RedisBool(value == 1ul);
        }

        public static implicit operator bool(RedisBool value)  // implicit RedisBool to bool conversion operator
        {
            return value.Value;
        }

        public static implicit operator byte(RedisBool value)  // implicit RedisBool to byte conversion operator
        {
            return value.Value ? (byte)1 : (byte)0;
        }

        public static implicit operator short(RedisBool value)  // implicit RedisBool to short conversion operator
        {
            return value.Value ? (short)1 : (short)0;
        }

        public static implicit operator int(RedisBool value)  // implicit RedisBool to int conversion operator
        {
            return value.Value ? 1 : 0;
        }

        public static implicit operator long(RedisBool value)  // implicit RedisBool to long conversion operator
        {
            return value.Value ? 1L : 0L;
        }

        public static implicit operator double(RedisBool value)  // implicit RedisBool to double conversion operator
        {
            return value.Value ? 1d : 0d;
        }

        public static implicit operator decimal(RedisBool value)  // implicit RedisBool to decimal conversion operator
        {
            return value.Value ? 1m : 0m;
        }

        public static implicit operator float(RedisBool value)  // implicit RedisBool to byte conversion operator
        {
            return value.Value ? 1f : 0f;
        }

        public static implicit operator ushort(RedisBool value)  // implicit RedisBool to ushort conversion operator
        {
            return value.Value ? (ushort)1 : (ushort)0;
        }

        public static implicit operator uint(RedisBool value)  // implicit RedisBool to uint conversion operator
        {
            return value.Value ? 1u : 0u;
        }

        public static implicit operator ulong(RedisBool value)  // implicit RedisBool to ulong conversion operator
        {
            return value.Value ? 1ul : 0ul;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(bool a, RedisBool b)
        {
            if (ReferenceEquals(a, null))
                return false;
            return (b.m_Status == RedisResultStatus.Completed) && ((bool)b.m_RawData == a);
        }

        public static bool operator !=(bool a, RedisBool b)
        {
            return b != a;
        }

        public static bool operator ==(RedisBool a, bool b)
        {
            if (ReferenceEquals(a, null))
                return false;
            return (a.m_Status == RedisResultStatus.Completed) && ((bool)a.m_RawData == b);
        }

        public static bool operator !=(RedisBool a, bool b)
        {
            return !(a == b);
        }

        public static bool operator ==(RedisBool a, RedisBool b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && ((bool)a.m_RawData == (bool)b.m_RawData);
        }

        public static bool operator !=(RedisBool a, RedisBool b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads    
    }
}
