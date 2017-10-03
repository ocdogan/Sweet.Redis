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

namespace Sweet.Redis
{
    public class RedisInt : RedisResult<long>
    {
        #region .Ctors

        internal RedisInt()
        { }

        internal RedisInt(long value)
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

            var rObj = obj as RedisInt;
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
            if (ReferenceEquals(value, null))
                return "(nil)";

            return ":" + value.ToString();
        }

        #endregion Overrides

        #endregion Methods

        #region Conversion Methods

        public static implicit operator RedisInt(long value)  // implicit long to RedisInt conversion operator
        {
            return new RedisInt(value);
        }

        public static implicit operator RedisInt(int value)  // implicit int to RedisInt conversion operator
        {
            return new RedisInt(value);
        }

        public static implicit operator RedisInt(double value)  // implicit double to RedisInt conversion operator
        {
            return new RedisInt((long)value);
        }

        public static implicit operator RedisInt(RedisDouble value)  // implicit RedisDouble to RedisInt conversion operator
        {
            return new RedisInt((long)value.Value);
        }

        public static implicit operator long(RedisInt value)  // implicit RedisInt to long conversion operator
        {
            return value.Value;
        }

        public static implicit operator int(RedisInt value)  // implicit RedisInt to int conversion operator
        {
            return (int)value.Value;
        }

        public static implicit operator double(RedisInt value)  // implicit RedisInt to double conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(RedisInt a, RedisInt b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && (a.m_RawData == b.m_RawData);
        }

        public static bool operator !=(RedisInt a, RedisInt b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
