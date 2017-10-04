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
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisResult<T> : RedisResult<T, T>
    {
        internal RedisResult()
            : base()
        { }

        internal RedisResult(T value)
            : base(value)
        { }
    }

    public class RedisResult<TValue, KItem> : RedisResult
    {
        #region .Ctors

        internal RedisResult()
        { }

        internal RedisResult(TValue value)
            : base(value)
        { }

        internal RedisResult(TValue value, RedisResultStatus status)
            : base(value, status)
        { }

        #endregion .Ctors

        #region Properties

        public virtual KItem this[int index]
        {
            get
            {
                ValidateCompleted();

                var val = Value;
                if (!Object.Equals(val, null))
                {
                    var list = val as IList<KItem>;
                    if (list != null)
                        return list[index];

                    var enumerable = val as IEnumerable<KItem>;
                    if (enumerable != null)
                    {
                        var i = 0;
                        foreach (var item in enumerable)
                        {
                            if (i++ == index)
                                return item;
                        }
                        throw new ArgumentOutOfRangeException("index", "Index value is out of range");
                    }

                    if (typeof(KItem) == typeof(TValue))
                        return ((KItem)(object)val);
                }
                throw new ArgumentOutOfRangeException("index", "Index value is out of range");
            }
        }

        public virtual TValue Value
        {
            get
            {
                ValidateCompleted();
                return (TValue)m_RawData;
            }
            internal set
            {
                m_RawData = value;
                IsCompleted = true;
            }
        }

        #endregion Properties

        #region Methods

        protected internal override void TrySetResult(object value)
        {
            if (!(value is TValue))
                throw new RedisException("Value is not in expected type");
            base.TrySetResult(value);
        }

        #region Overrides

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        #endregion Overrides

        #endregion Methods

        #region Operator Overloads

        public static bool operator ==(RedisResult<TValue, KItem> a, RedisResult<TValue, KItem> b)
        {
            if (ReferenceEquals(a, null))
            {
                if (ReferenceEquals(b, null))
                    return true;

                var raw = b.m_RawData;
                return ReferenceEquals(raw, null) || Object.Equals(raw, null);
            }

            if (ReferenceEquals(b, null))
            {
                var raw = a.m_RawData;
                return ReferenceEquals(raw, null) || Object.Equals(raw, null);
            }

            if (ReferenceEquals(a, b))
                return true;

            return (a.Status == b.Status) && Object.Equals(a.m_RawData, b.m_RawData);
        }

        public static bool operator !=(RedisResult<TValue, KItem> a, RedisResult<TValue, KItem> b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }

    public class RedisResult
    {
        #region Field Members

        protected object m_RawData;
        protected RedisResultStatus m_Status = RedisResultStatus.Pending;

        #endregion Field Members

        #region .Ctors

        internal RedisResult()
        { }

        internal RedisResult(object value)
            : this()
        {
            m_RawData = value;
            m_Status = RedisResultStatus.Completed;
        }

        internal RedisResult(object value, RedisResultStatus status)
            : this()
        {
            m_RawData = value;
            m_Status = status;
        }

        #endregion .Ctors

        #region Properties

        public bool IsCompleted
        {
            get { return m_Status == RedisResultStatus.Completed; }
            protected set
            {
                m_Status = value ? RedisResultStatus.Completed : RedisResultStatus.Pending;
            }
        }

        public virtual int Length
        {
            get
            {
                ValidateCompleted();
                return 0;
            }
        }

        protected virtual object RawData
        {
            get
            {
                return m_RawData;
            }
            set
            {
                m_RawData = value;
                m_Status = RedisResultStatus.Completed;
            }
        }

        public RedisResultStatus Status
        {
            get { return m_Status; }
            internal set { m_Status = value; }
        }

        public virtual RedisResultType Type 
        { 
            get { return RedisResultType.Custom; } 
        }

        #endregion Properties

        #region Methods

        protected virtual void ValidateCompleted()
        {
            if (!IsCompleted)
                throw new RedisException("Result is not completed");
        }

        internal void SetCompleted()
        {
            m_Status = RedisResultStatus.Completed;
        }

        protected internal virtual void TrySetResult(object value)
        {
            m_RawData = value;
            m_Status = RedisResultStatus.Completed;
        }

        #region Overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
            {
                var value = m_RawData;
                return ReferenceEquals(value, null) || (value == null);
            }

            if (ReferenceEquals(obj, this))
                return true;

            if (obj is RedisResult &&
               Object.Equals(m_RawData, ((RedisResult)obj).m_RawData))
                return true;

            return Object.Equals(m_RawData, obj);
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
            if (!IsCompleted)
                return "(nil) - [Not completed]";

            var value = m_RawData;
            if (ReferenceEquals(value, null))
                return "(nil)";

            if (value is IEnumerable)
            {
                var enumerable = (IEnumerable)value;

                var i = 0;
                var sBuilder = new StringBuilder();

                foreach (var item in enumerable)
                {
                    sBuilder.Append(++i);
                    sBuilder.Append(") ");

                    if (item == null)
                        sBuilder.Append("(nil)");
                    else
                        sBuilder.Append(item);

                    sBuilder.AppendLine();
                }

                return sBuilder.ToString();
            }

            return value.ToString();
        }

        #endregion Overrides

        #endregion Methods

        #region Operator Overloads

        public static bool operator ==(RedisResult a, RedisResult b)
        {
            if (ReferenceEquals(a, null))
            {
                if (ReferenceEquals(b, null))
                    return true;

                var raw = b.m_RawData;
                return ReferenceEquals(raw, null) || Object.Equals(raw, null);
            }

            if (ReferenceEquals(b, null))
            {
                var raw = a.m_RawData;
                return ReferenceEquals(raw, null) || Object.Equals(raw, null);
            }

            if (ReferenceEquals(a, b))
                return true;

            return (a.Status == b.Status) && Object.Equals(a.m_RawData, b.m_RawData);
        }

        public static bool operator !=(RedisResult a, RedisResult b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
