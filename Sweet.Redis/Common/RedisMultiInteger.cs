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
    public class RedisMultiInteger : RedisResult<long[], long>
    {
        #region .Ctors

        internal RedisMultiInteger()
        { }

        internal RedisMultiInteger(long[] value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override long this[int index]
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

        public override RedisResultType Type { get { return RedisResultType.MultiInt; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var rObj = obj as RedisMultiInteger;
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

            var longs = value as long[];
            if (longs == null)
                return "(nil)";

            var length = longs.Length;
            if (length == 0)
                return "(empty)";

            var sBuilder = new StringBuilder();

            for (var i = 0; i < length; i++)
            {
                sBuilder.Append(i + 1);
                sBuilder.Append(") :");
                sBuilder.Append(longs[i]);

                sBuilder.AppendLine();
            }

            return sBuilder.ToString();
        }

        #endregion Overrides

        #endregion Methods

        #region Conversion Methods

        public static implicit operator RedisMultiInteger(long[] value)  // implicit long[] to RedisMultiInt conversion operator
        {
            return new RedisMultiInteger(value);
        }

        public static implicit operator long[] (RedisMultiInteger value)  // implicit RedisMultiInt to long[] conversion operator
        {
            return value.Value;
        }

        public static implicit operator RedisMultiInteger(int[] value)  // implicit int[] to RedisMultiInt conversion operator
        {
            long[] longs = null;
            if (value != null)
            {
                var length = value.Length;

                longs = new long[length];
                if (length > 0)
                    Buffer.BlockCopy(value, 0, longs, 0, length);
            }
            return new RedisMultiInteger(longs);
        }

        public static implicit operator RedisMultiInteger(short[] value)  // implicit short[] to RedisMultiInt conversion operator
        {
            long[] longs = null;
            if (value != null)
            {
                var length = value.Length;

                longs = new long[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        longs[i] = (long)value[i];
            }
            return new RedisMultiInteger(longs);
        }

        public static implicit operator RedisMultiInteger(double[] value)  // implicit double[] to RedisMultiInt conversion operator
        {
            long[] longs = null;
            if (value != null)
            {
                var length = value.Length;

                longs = new long[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        longs[i] = (long)value[i];
            }
            return new RedisMultiInteger(longs);
        }

        public static implicit operator RedisMultiInteger(decimal[] value)  // implicit decimal[] to RedisMultiInt conversion operator
        {
            long[] longs = null;
            if (value != null)
            {
                var length = value.Length;

                longs = new long[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        longs[i] = Convert.ToInt64(value[i]);
            }
            return new RedisMultiInteger(longs);
        }

        public static implicit operator RedisMultiInteger(float[] value)  // implicit float[] to RedisMultiInt conversion operator
        {
            long[] longs = null;
            if (value != null)
            {
                var length = value.Length;

                longs = new long[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        longs[i] = Convert.ToInt64(value[i]);
            }
            return new RedisMultiInteger(longs);
        }

        public static implicit operator int[](RedisMultiInteger value)  // implicit RedisMultiInt to int[] conversion operator
        {
            var longs = value.Value;
            if (longs != null)
            {
                var length = longs.Length;

                var result = new int[length];
                if (length > 0)
                    Buffer.BlockCopy(longs, 0, result, 0, length);
            }
            return null;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisMultiInteger a, RedisMultiInteger b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && (a.m_RawData == b.m_RawData);
        }

        public static bool operator !=(RedisMultiInteger a, RedisMultiInteger b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
