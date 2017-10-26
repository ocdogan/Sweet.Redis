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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisManagedConnectionPool : RedisConnectionPool
    {
        #region Field Members

        private bool m_SDown;
        private bool m_ODown;

        private RedisRole m_Role;

        #endregion Field Members

        #region .Ctors

        public RedisManagedConnectionPool(RedisRole role, string name, RedisPoolSettings settings)
            : base(name, settings)
        {
            Role = role;
        }

        #endregion .Ctors

        #region Properties

        public bool IsDown
        {
            get { return m_SDown || m_ODown; }
        }

        public bool ODown
        {
            get { return m_ODown; }
            set { m_ODown = value; }
        }

        public RedisRole Role
        {
            get { return m_Role; }
            internal set
            {
                m_Role = value;
                ApplyRole(value);
            }
        }

        public bool SDown
        {
            get { return m_SDown; }
            set { m_SDown = value; }
        }

        #endregion Properties

        #region Methods

        protected override void OnBeforeConnect(int dbIndex, RedisRole expectedRole)
        {
            if (IsDown) throw new RedisFatalException("Pool is down");
        }

        #endregion Methods
    }
}
