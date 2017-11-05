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
    public class RedisManagedPool : RedisConnectionPool
    {
        #region Field Members

        private bool m_SDown;
        private bool m_ODown;

        private RedisRole m_Role;

        #endregion Field Members

        #region .Ctors

        public RedisManagedPool(RedisRole role, string name, RedisPoolSettings settings)
            : base(name, settings)
        {
            Role = role;
        }

        #endregion .Ctors

        #region Properties

        public override bool IsDown
        {
            get { return m_SDown || m_ODown || Disposed; }
            protected internal set
            {
                if (!Disposed || !value)
                {
                    var wasDown = IsDown;

                    m_SDown = value;
                    m_ODown = value;

                    if (IsDown != wasDown && !Disposed)
                        DownStateChanged(!wasDown);
                }
            }
        }

        public bool ODown
        {
            get { return m_ODown || Disposed; }
            set
            {
                if (m_ODown != value && (!Disposed || !value))
                {
                    var wasDown = IsDown;

                    m_ODown = value;
                    m_SDown = value;

                    if (IsDown != wasDown && !Disposed)
                        DownStateChanged(!wasDown);
                }
            }
        }

        public RedisRole Role
        {
            get { return m_Role; }
            internal set
            {
                if (!Disposed)
                {
                    m_Role = value;
                    ApplyRole(value);
                }
            }
        }

        public bool SDown
        {
            get { return m_SDown || Disposed; }
            set
            {
                if (m_SDown != value && (!Disposed || !value))
                {
                    var wasDown = IsDown;
                    m_SDown = value;
                    if (IsDown != wasDown && !Disposed)
                        DownStateChanged(!wasDown);
                }
            }
        }

        #endregion Properties

        #region Methods

        protected override void OnBeforeConnect(int dbIndex, RedisRole expectedRole)
        {
            if (IsDown) throw new RedisFatalException("Pool is down");
        }

        protected virtual void DownStateChanged(bool down)
        {
        }

        #endregion Methods
    }
}
