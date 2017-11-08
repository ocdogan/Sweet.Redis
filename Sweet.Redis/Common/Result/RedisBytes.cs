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
    public class RedisBytes : RedisResult<byte[], byte>
    {
        #region .Ctors

        internal RedisBytes()
        { }

        public RedisBytes(byte[] value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override byte this[int index]
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

        public override RedisResultType Type { get { return RedisResultType.Bytes; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var bObj = obj as RedisBytes;
            if (!ReferenceEquals(bObj, null))
                return (bObj.Status == m_Status) && ((byte[])bObj.RawData).EqualTo((byte[])m_RawData);

            var bytes = obj as byte[];
            if (!ReferenceEquals(bytes, null))
                return bytes.EqualTo((byte[])m_RawData);

            var sObj = obj as RedisString;
            if (!ReferenceEquals(sObj, null))
                return (sObj.Status == m_Status) && ((byte[])sObj.RawData).EqualTo(((string)m_RawData).ToBytes());

            var str = obj as string;
            if (!ReferenceEquals(str, null))
                return ((byte[])m_RawData).EqualTo(str.ToBytes());

            var eObj = obj as RedisError;
            if (!ReferenceEquals(eObj, null))
                return (eObj.Status == m_Status) && ((byte[])eObj.RawData).EqualTo(((string)m_RawData).ToBytes());

            var rObj = obj as RedisResult<string>;
            if (!ReferenceEquals(rObj, null))
                return (rObj.Status == m_Status) && ((byte[])rObj.RawData).EqualTo(((string)m_RawData).ToBytes());

            var rbObj = obj as RedisResult<byte[]>;
            if (!ReferenceEquals(rbObj, null))
                return (rbObj.Status == m_Status) && ((byte[])rbObj.RawData).EqualTo((byte[])m_RawData);

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
            var value = (byte[])m_RawData;
            if (ReferenceEquals(value, null))
                return "(nil)";

            if (value.Length == 0)
                return "(empty)";

            return "\"" + value.ToUTF8String() + "\"";
        }

        #endregion Methods

        #endregion Overrides

        #region Conversion Methods

        #region To RedisBytes

        public static implicit operator RedisBytes(byte[] value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value);
        }

        public static implicit operator RedisBytes(string value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(long value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(int value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(short value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(double value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(decimal value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(float value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(ulong value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(uint value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(ushort value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(bool value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value ? "1".ToBytes() : "0".ToBytes());
        }

        public static implicit operator RedisBytes(DateTime value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.ToBytes());
        }

        public static implicit operator RedisBytes(TimeSpan value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.Ticks.ToBytes());
        }

        public static implicit operator RedisBytes(long? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(int? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(short? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(double? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(decimal? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(float? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(ulong? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(uint? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(ushort? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(DateTime? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.ToBytes() : null);
        }

        public static implicit operator RedisBytes(TimeSpan? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? value.Value.Ticks.ToBytes() : null);
        }

        public static implicit operator RedisBytes(bool? value)  // implicit to RedisBytes conversion operator
        {
            return new RedisBytes(value.HasValue ? (value.Value ? "1".ToBytes() : "0".ToBytes()) : null);
        }

        #endregion To RedisBytes

        #region From RedisBytes

        public static implicit operator byte[] (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return !ReferenceEquals(value, null) ? value.Value : null;
        }

        public static implicit operator string(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return !ReferenceEquals(value, null) ? value.Value.ToUTF8String() : null;
        }

        public static implicit operator long(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(long) : long.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator int(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(int) : int.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator short(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(short) : short.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator double(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(double) : double.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator decimal(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(decimal) : decimal.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator float(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(float) : float.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator ulong(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(ulong) : ulong.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator uint(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(uint) : uint.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator ushort(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(ushort) : ushort.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator bool(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(bool) : value.Value.ToUTF8String() == "1";
        }

        public static implicit operator DateTime(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(DateTime) : new DateTime(long.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture));
        }

        public static implicit operator TimeSpan(RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? default(TimeSpan) : new TimeSpan(long.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture));
        }

        public static implicit operator long? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (long?)null : long.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator int? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (int?)null : int.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator short? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (short?)null : short.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator double? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (double?)null : double.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator decimal? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (decimal?)null : decimal.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator float? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (float?)null : float.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator ulong? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (ulong?)null : ulong.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator uint? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (uint?)null : uint.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator ushort? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (ushort?)null : ushort.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture);
        }

        public static implicit operator bool? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (bool?)null : value.Value.ToUTF8String() == "1";
        }

        public static implicit operator DateTime? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (DateTime?)null : new DateTime(long.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture));
        }

        public static implicit operator TimeSpan? (RedisBytes value)  // implicit from RedisBytes conversion operator
        {
            return ReferenceEquals(value, null) || value.Value == null ? (TimeSpan?)null : new TimeSpan(long.Parse(value.Value.ToUTF8String(), RedisConstants.InvariantCulture));
        }

        #endregion From RedisBytes

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(byte[] a, RedisBytes b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (b.m_Status == RedisResultStatus.Completed) && (((byte[])b.m_RawData == a) ||
                ((RedisByteArray)b.m_RawData == (RedisByteArray)a));
        }

        public static bool operator !=(byte[] a, RedisBytes b)
        {
            return !(b == a);
        }

        public static bool operator ==(RedisBytes a, byte[] b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == RedisResultStatus.Completed) && (((byte[])a.m_RawData == b) ||
                ((RedisByteArray)a.m_RawData == (RedisByteArray)b));
        }

        public static bool operator !=(RedisBytes a, byte[] b)
        {
            return !(a == b);
        }

        public static bool operator ==(RedisBytes a, RedisBytes b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && (((byte[])a.m_RawData == (byte[])b.m_RawData) || 
                ((RedisByteArray)a.m_RawData == (RedisByteArray)b.m_RawData));
        }

        public static bool operator !=(RedisBytes a, RedisBytes b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
