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
    public struct RedisNodeInfo : IEquatable<RedisNodeInfo>
    {
        #region Static Members

        public static readonly RedisNodeInfo Empty = new RedisNodeInfo(null, RedisRole.Undefined);

        #endregion Static Members

        #region .Ctors

        public RedisNodeInfo(RedisEndPoint endPoint, RedisRole role, string name = null)
            : this()
        {
            Role = role;
            Name = name ?? String.Empty;
            EndPoint = endPoint ?? RedisEndPoint.Empty;
        }

        #endregion .Ctors

        #region Properties

        public bool IsEmpty
        {
            get
            {
                return (Role == RedisRole.Undefined) &&
                    EndPoint.IsEmpty() && Name.IsEmpty();
            }
        }

        public string Name { get; private set; }

        public RedisEndPoint EndPoint { get; private set; }

        public RedisRole Role { get; private set; }

        #endregion Properties

        #region Methods

        #region Overrides

        public override string ToString()
        {
            return String.Format("[RedisNodeInfo: Name={0}, EndPoint={1}, Role={2}]", Name, EndPoint, Role);
        }

        public override int GetHashCode()
        {
            var hash = 13;
            hash = (hash * 7) + Role.GetHashCode();

            var endPoint = EndPoint;
            if (!endPoint.IsEmpty())
                hash = (hash * 7) + endPoint.GetHashCode();

            var name = Name;
            if (!name.IsEmpty())
                hash = (hash * 7) + name.GetHashCode();

            return hash;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            if (ReferenceEquals(obj, this))
                return true;

            if (obj is RedisNodeInfo)
            {
                var other = (RedisNodeInfo)obj;
                return Role == other.Role && EndPoint == other.EndPoint &&
                     String.Equals(Name, other.Name, StringComparison.Ordinal);
            }
            return false;
        }

        public bool Equals(RedisNodeInfo other)
        {
            return Role == other.Role && EndPoint == other.EndPoint &&
                 String.Equals(Name, other.Name, StringComparison.Ordinal);
        }

        #endregion Overrides

        #endregion Methods

        #region Operator Overloads

        public static bool operator ==(RedisNodeInfo a, RedisNodeInfo b)
        {
            return a.Equals(b);
        }

        public static bool operator !=(RedisNodeInfo a, RedisNodeInfo b)
        {
            return !(a == b);
        }

        #endregion Operator Overloads

    }
}
