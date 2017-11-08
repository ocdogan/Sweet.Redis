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

using System.Threading;

namespace Sweet.Redis
{
    public class RedisSocketContext : RedisInternalDisposable
    {
        #region Field Members

        private RedisSocket m_Socket;
        private RedisConnectionSettings m_Settings;

        #endregion Field Members

        #region .Ctors

        internal RedisSocketContext(RedisSocket socket, RedisConnectionSettings settings)
        {
            m_Socket = socket;
            m_Settings = settings ?? RedisConnectionSettings.Default;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Socket, null);
            Interlocked.Exchange(ref m_Settings, null);

            base.OnDispose(disposing);
        }

        #endregion Destructors

        #region Properties

        public int DbIndex
        {
            get { return (m_Socket != null) ? m_Socket.DbIndex : -1; }
        }

        public RedisSocket Socket 
        { 
            get { return m_Socket; } 
        }

        public RedisConnectionSettings Settings 
        { 
            get { return m_Settings; } 
        }

        #endregion Properties
    }
}
