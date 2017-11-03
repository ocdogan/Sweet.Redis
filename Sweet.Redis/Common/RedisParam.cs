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
    public struct RedisParam
    {
        #region Static Members

        public static readonly RedisParam Empty = new RedisParam((byte[])null);

        #endregion Static Members

        #region Field Members

        private byte[] m_Data;

        #endregion Field Members

        #region .Ctors

        public RedisParam(byte[] data)
            : this()
        {
            m_Data = data;
        }

        public RedisParam(string data)
            : this()
        {
            m_Data = (data != null) ? data.ToBytes() : null;
        }

        public RedisParam(DateTime? data)
            : this()
        {
            m_Data = (data != null) ? data.Value.Ticks.ToBytes() : null;
        }

        public RedisParam(byte? data)
            : this()
        {
            m_Data = (data != null) ? data.ToBytes() : null;
        }

        public RedisParam(short? data)
            : this()
        {
            m_Data = (data != null) ? data.ToBytes() : null;
        }

        public RedisParam(int? data)
            : this()
        {
            m_Data = (data != null) ? data.ToBytes() : null;
        }

        public RedisParam(long? data)
            : this()
        {
            m_Data = (data != null) ? data.ToBytes() : null;
        }

        public RedisParam(double? data)
            : this()
        {
            m_Data = (data != null) ? data.ToBytes() : null;
        }

        public RedisParam(bool? data)
            : this()
        {
            m_Data = (data != null) ? data.ToBytes() : null;
        }

        public RedisParam(DateTime data)
            : this()
        {
            m_Data = data.Ticks.ToBytes();
        }

        public RedisParam(byte data)
            : this()
        {
            m_Data = data.ToBytes();
        }

        public RedisParam(short data)
            : this()
        {
            m_Data = data.ToBytes();
        }

        public RedisParam(int data)
            : this()
        {
            m_Data = data.ToBytes();
        }

        public RedisParam(long data)
            : this()
        {
            m_Data = data.ToBytes();
        }

        public RedisParam(double data)
            : this()
        {
            m_Data = data.ToBytes();
        }

        public RedisParam(bool data)
            : this()
        {
            m_Data = data.ToBytes();
        }

        #endregion .Ctors

        #region Properties

        public byte[] Data
        {
            get { return m_Data; }
            private set { m_Data = value; }
        }

        public bool IsNull
        {
            get { return m_Data == null; }
        }

        public bool IsEmpty
        {
            get { return m_Data.IsEmpty(); }
        }

        public int Length
        {
            get { return m_Data != null ? m_Data.Length : 0; }
        }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            if (obj is RedisParam)
            {
                var rObj = (RedisParam)obj;
                return (rObj.Data == m_Data);
            }
            return false;
        }

        public override int GetHashCode()
        {
            var value = m_Data;
            if (ReferenceEquals(value, null))
                return base.GetHashCode();
            return value.GetHashCode();
        }

        public override string ToString()
        {
            var value = m_Data;
            if (ReferenceEquals(value, null))
                return "(nil)";

            if (value.Length == 0)
                return "(empty)";

            return "\"" + Encoding.UTF8.GetString(value) + "\"";
        }

        #endregion Methods

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator RedisParam(byte[] data)  // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(string data)  // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(DateTime? data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(byte? data)   // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(short? data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(int? data)  // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(long? data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(double? data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(bool? data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(DateTime data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(byte data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(short data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(int data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(long data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(double data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator RedisParam(bool data) // implicit ? type to RedisParam conversion operator
        {
            return new RedisParam(data);
        }

        public static implicit operator byte[] (RedisParam param)  // implicit RedisParam to ? type conversion operator
        {
            return param.Data;
        }

        public static implicit operator string(RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return param.Data != null ? Encoding.UTF8.GetString(param.Data) : null;
        }

        public static implicit operator DateTime? (RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return !param.Data.IsEmpty() ? (DateTime?)(new DateTime(long.Parse(Encoding.UTF8.GetString(param.Data)))) : null;
        }

        public static implicit operator byte? (RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return !param.Data.IsEmpty() ? (byte?)param.Data[0] : null;
        }

        public static implicit operator short? (RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return !param.Data.IsEmpty() ? (short?)short.Parse(Encoding.UTF8.GetString(param.Data)) : null;
        }

        public static implicit operator int? (RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return !param.Data.IsEmpty() ? (int?)int.Parse(Encoding.UTF8.GetString(param.Data)) : null;
        }

        public static implicit operator long? (RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return !param.Data.IsEmpty() ? (long?)long.Parse(Encoding.UTF8.GetString(param.Data)) : null;
        }

        public static implicit operator double? (RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return !param.Data.IsEmpty() ? (double?)double.Parse(Encoding.UTF8.GetString(param.Data)) : null;
        }

        public static implicit operator bool? (RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return !param.Data.IsEmpty() ? (bool?)bool.Parse(Encoding.UTF8.GetString(param.Data)) : null;
        }

        public static implicit operator DateTime(RedisParam param) // implicit RedisParam to ? type conversion operator
        {
            return new DateTime(long.Parse(Encoding.UTF8.GetString(param.Data)));
        }

        public static implicit operator byte(RedisParam param) // implicit RedisParam to  type conversion operator
        {
            return param.Data[0];
        }

        public static implicit operator short(RedisParam param) // implicit RedisParam to  type conversion operator
        {
            return short.Parse(Encoding.UTF8.GetString(param.Data));
        }

        public static implicit operator int(RedisParam param) // implicit RedisParam to  type conversion operator
        {
            return int.Parse(Encoding.UTF8.GetString(param.Data));
        }

        public static implicit operator long(RedisParam param) // implicit RedisParam to  type conversion operator
        {
            return long.Parse(Encoding.UTF8.GetString(param.Data));
        }

        public static implicit operator double(RedisParam param) // implicit RedisParam to  type conversion operator
        {
            return double.Parse(Encoding.UTF8.GetString(param.Data));
        }

        public static implicit operator bool(RedisParam param) // implicit RedisParam to  type conversion operator
        {
            return bool.Parse(Encoding.UTF8.GetString(param.Data));
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(object a, RedisParam b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return b.m_Data.EqualTo(a);
        }

        public static bool operator !=(object a, RedisParam b)
        {
            return !(b == a);
        }

        public static bool operator ==(RedisParam a, object b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return a.m_Data.EqualTo(b);
        }

        public static bool operator !=(RedisParam a, object b)
        {
            return !(a == b);
        }

        public static bool operator ==(RedisParam a, RedisParam b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Data == b.m_Data);
        }

        public static bool operator !=(RedisParam a, RedisParam b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
