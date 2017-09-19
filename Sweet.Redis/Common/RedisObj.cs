﻿#region License
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
    public class RedisObj : RedisResult<object>
    {
        #region .Ctors

        internal RedisObj()
        { }

        internal RedisObj(object value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Object; } }

        #endregion Properties
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

            if (obj is RedisObj)
                return Object.Equals(Value, ((RedisObj)obj).Value);

            return Object.Equals(Value, obj);
        }

        public override int GetHashCode()
        {
            var val = Value;
            if (ReferenceEquals(val, null))
                return base.GetHashCode();
            return val.GetHashCode();
        }

        public static bool operator ==(RedisObj a, RedisObj b)
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

        public static bool operator !=(RedisObj a, RedisObj b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
