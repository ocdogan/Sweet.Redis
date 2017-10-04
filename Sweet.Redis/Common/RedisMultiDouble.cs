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
    public class RedisMultiDouble : RedisResult<double[], double>
    {
        #region .Ctors

        internal RedisMultiDouble()
        { }

        internal RedisMultiDouble(double[] value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.MultiDouble; } }

        #endregion Properties

        #region Methods

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var rObj = obj as RedisMultiDouble;
            if (!ReferenceEquals(rObj, null))
                return (rObj.m_Status == m_Status) && Object.Equals(rObj.m_RawData, m_RawData);
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

            return "\"" + value.ToString() + "\"";
        }
        #endregion Methods

        #endregion Overrides

        #region Conversion Methods

        public static implicit operator double[](RedisMultiDouble value)  // implicit RedisMultiDouble to double conversion operator
        {
            return value.Value;
        }

        public static implicit operator RedisMultiDouble(double[] value)  // implicit double to RedisMultiDouble conversion operator
        {
            return new RedisMultiDouble(value);
        }

        public static implicit operator RedisMultiDouble(long[] value)  // implicit long to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    Buffer.BlockCopy(value, 0, doubles, 0, length);
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(int[] value)  // implicit int to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        doubles[i] = (double)value[i];
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(short[] value)  // implicit short to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        doubles[i] = (double)value[i];
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(decimal[] value)  // implicit decimal to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        doubles[i] = Convert.ToDouble(value[i]);
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(float[] value)  // implicit float to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        doubles[i] = Convert.ToDouble(value[i]);
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(ulong[] value)  // implicit ulong to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    Buffer.BlockCopy(value, 0, doubles, 0, length);
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(uint[] value)  // implicit uint to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        doubles[i] = (double)value[i];
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(ushort[] value)  // implicit ushort to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        doubles[i] = (double)value[i];
            }
            return new RedisMultiDouble(doubles);
        }

        public static implicit operator RedisMultiDouble(RedisInteger value)  // implicit int to RedisMultiDouble conversion operator
        {
            double[] doubles = null;
            if (value != null)
            {
                var length = value.Length;

                doubles = new double[length];
                if (length > 0)
                    for (var i = 0; i < length; i++)
                        doubles[i] = (double)value[i];
            }
            return new RedisMultiDouble(doubles);
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisMultiDouble a, RedisMultiDouble b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && Object.Equals(a.m_RawData, b.m_RawData);
        }

        public static bool operator !=(RedisMultiDouble a, RedisMultiDouble b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
