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
using System.Runtime.Serialization;

namespace Sweet.Redis
{
    [Serializable]
    public class RedisException : Exception
    {
        private string m_Prefix;

        public RedisException()
            : base()
        { }

        public RedisException(string message)
            : base(message)
        { }

        public RedisException(string prefix, string message)
            : base(message)
        {
            Prefix = prefix;
        }

        public RedisException(string message, Exception innerException)
            : base(message, innerException)
        { }

        public RedisException(string prefix, string message, Exception innerException)
            : base(message, innerException)
        {
            Prefix = prefix;
        }

        public RedisException(string message, Exception innerException, params object[] args)
            : base(string.Format(message, args), innerException)
        { }

        public RedisException(string prefix, string message, Exception innerException, params object[] args)
            : base(string.Format(message, args), innerException)
        {
            Prefix = prefix;
        }

        protected RedisException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        { }

        public string Prefix
        {
            get { return String.IsNullOrEmpty(m_Prefix) ? "ERR" : m_Prefix; }
            set
            {
                if (value == null)
                {
                    m_Prefix = null;
                }
                else
                {
                    m_Prefix = value.Trim();
                    if (m_Prefix == String.Empty)
                        m_Prefix = null;
                }
            }
        }
    }
}
