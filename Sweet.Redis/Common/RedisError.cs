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
    public class RedisError : RedisResult<string, char>
    {
        #region .Ctors

        internal RedisError()
        { }

        internal RedisError(string value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override char this[int index]
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

        public override RedisResultType Type { get { return RedisResultType.Error; } }

        #endregion Properties

        #region Conversion Methods

        public static implicit operator RedisError(string value)  // implicit string to RedisError conversion operator
        {
            return new RedisError(value);
        }

        public static implicit operator string(RedisError value)  // implicit RedisError to string conversion operator
        {
            return value.Value;
        }

        #endregion Conversion Methods

        #region Operator Overloads

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            var rObj = obj as RedisError;
            if (!ReferenceEquals(rObj, null))
                return (rObj.m_Status == m_Status) && (rObj.m_Value == m_Value);
            return false;
        }

        public override int GetHashCode()
        {
            var val = Value;
            if (ReferenceEquals(val, null))
                return base.GetHashCode();
            return val.GetHashCode();
        }

        public static bool operator ==(RedisError a, RedisError b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return (a.m_Status == b.m_Status) && (a.m_Value == b.m_Value);
        }

        public static bool operator !=(RedisError a, RedisError b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
