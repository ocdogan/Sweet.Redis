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
    public class RedisMultiInt : RedisResult<long[], long>
    {
        #region .Ctors

        internal RedisMultiInt()
        { }

        internal RedisMultiInt(long[] value)
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

        #region Conversion Methods

        public static implicit operator RedisMultiInt(long[] value)  // implicit long[] to RedisMultiInt conversion operator
        {
            return new RedisMultiInt(value);
        }

        public static implicit operator long[](RedisMultiInt value)  // implicit RedisMultiInt to long[] conversion operator
        {
            return value.Value;
        }

        public static implicit operator RedisMultiInt(int[] value)  // implicit int[] to RedisMultiInt conversion operator
        {
            long[] longs = null;
            if (value != null)
            {
                var length = value.Length;

                longs = new long[length];
                if (length > 0)
                    Buffer.BlockCopy(value, 0, longs, 0, length);
            }
            return new RedisMultiInt(longs);
        }

        public static implicit operator int[](RedisMultiInt value)  // implicit RedisMultiInt to int[] conversion operator
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

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
            {
                var val = Value;
                return ReferenceEquals(val, null) || (val == null);
            }

            if (ReferenceEquals(obj, this))
                return true;

            if (obj is RedisMultiInt)
                return Object.Equals(Value, ((RedisMultiInt)obj).Value);

            return Object.Equals(Value, obj);
        }

        public override int GetHashCode()
        {
            var val = Value;
            if (ReferenceEquals(val, null))
                return base.GetHashCode();
            return val.GetHashCode();
        }

        public static bool operator ==(RedisMultiInt a, RedisMultiInt b)
        {
            if (ReferenceEquals(a, null))
            {
                if (ReferenceEquals(b, null))
                    return true;

                var val = b.Value;
                return ReferenceEquals(val, null) || (val == null);
            }

            if (ReferenceEquals(b, null))
            {
                var val = a.Value;
                return ReferenceEquals(val, null) || (val == null);
            }

            return Object.Equals(a.Value, b.Value);
        }

        public static bool operator !=(RedisMultiInt a, RedisMultiInt b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
