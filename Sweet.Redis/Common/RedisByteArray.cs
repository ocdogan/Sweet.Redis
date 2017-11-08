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
    public class RedisByteArray : IEquatable<RedisByteArray>
    {
        #region Field Members

        private int? m_Hash;
        private byte[] m_Bytes;

        #endregion Field Members

        #region .Ctors

        public RedisByteArray(byte[] bytes)
        {
            m_Bytes = bytes;
        }

        #endregion .Ctors

        #region Properties

        public byte[] Bytes
        {
            get { return m_Bytes; }
        }

        #endregion Properties

        #region Methods

        public bool Equals(RedisByteArray other)
        {
            if (!ReferenceEquals(other, null))
            {
                if (ReferenceEquals(other, this))
                    return true;

                if (GetHashCode() == other.GetHashCode())
                    return m_Bytes.EqualTo(other.m_Bytes);
            }

            return false;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return (m_Bytes == null);

            var rba = obj as RedisByteArray;
            if (!ReferenceEquals(rba, null))
                return this.Equals(rba);

            var ba = obj as byte[];
            if (!ReferenceEquals(rba, null))
                return m_Bytes.EqualTo(ba);

            return false;
        }

        public override int GetHashCode()
        {
            if (!m_Hash.HasValue)
            {
                var hash = 0;
                var seed = 314;

                if (m_Bytes != null)
                {
                    var length = m_Bytes.Length;
                    if (length > 0)
                    {
                        for (var i = 0; i < length; i++)
                        {
                            hash = (hash * seed) + m_Bytes[i];
                            seed *= 159;
                        }
                    }
                }
                m_Hash = hash;
            }
            return m_Hash.Value;
        }
       
        #endregion Methods

        #region Conversion Methods

        #region To RedisByteArray

        public static implicit operator RedisByteArray(byte[] value)  // implicit to RedisByteArray conversion operator
        {
            return new RedisByteArray(value);
        }

        public static implicit operator RedisByteArray(string value)  // implicit to RedisByteArray conversion operator
        {
            return new RedisByteArray(value.ToBytes());
        }

        #endregion To RedisByteArray

        #region From RedisByteArray

        public static implicit operator byte[](RedisByteArray value)  // implicit from RedisByteArray conversion operator
        {
            return !ReferenceEquals(value, null) ? value.Bytes : null;
        }

        public static implicit operator string(RedisByteArray value)  // implicit from RedisByteArray conversion operator
        {
            return ReferenceEquals(value, null) ? null : value.Bytes.ToUTF8String();
        }

        #endregion From RedisByteArray

        #endregion Conversion Methods

        #region Operator Overloads

        public static bool operator ==(byte[] a, RedisByteArray b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            return a.EqualTo(b.m_Bytes);
        }

        public static bool operator !=(byte[] a, RedisByteArray b)
        {
            return !(b == a);
        }

        public static bool operator ==(RedisByteArray a, byte[] b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            return b.EqualTo(a.m_Bytes);
        }

        public static bool operator !=(RedisByteArray a, byte[] b)
        {
            return !(a == b);
        }

        public static bool operator ==(RedisByteArray a, RedisByteArray b)
        {
            if (ReferenceEquals(a, null))
                return ReferenceEquals(b, null);

            if (ReferenceEquals(b, null))
                return false;

            if (ReferenceEquals(a, b))
                return true;

            return a.Equals(b);
        }

        public static bool operator !=(RedisByteArray a, RedisByteArray b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads
    }
}
