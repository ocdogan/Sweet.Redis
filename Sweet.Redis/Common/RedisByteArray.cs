using System;
using System.Collections.Generic;
using System.Linq;
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
                    return Equals(m_Bytes, other.m_Bytes);
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
                return RedisByteArray.Equals(ba, m_Bytes);

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

        public static bool Equals(byte[] x, byte[] y)
        {
            if (x == y)
                return true;

            if (x == null)
                return y == null;

            if (y == null)
                return false;

            var l1 = x.Length;
            var l2 = y.Length;

            if (l1 != l2)
                return false;

            for (var i = 0; i < l1; i++)
                if (x[i] != y[i])
                    return false;

            return true;
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
            return value != (byte[])null ? value.Bytes : null;
        }

        public static implicit operator string(RedisByteArray value)  // implicit from RedisByteArray conversion operator
        {
            return value == (byte[])null || value.Bytes == null ? null : Encoding.UTF8.GetString(value.Bytes);
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

            return RedisByteArray.Equals(b.m_Bytes, a);
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

            return RedisByteArray.Equals(a.m_Bytes, b);
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
