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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal abstract class RedisConnection : RedisDisposable, IRedisConnection
    {
        #region Constants

        private const int SIO_LOOPBACK_FAST_PATH = -1744830448;

        #endregion Constants

        #region Field Members

        protected RedisSocket m_Socket;
        protected EndPoint m_EndPoint;
        protected RedisSettings m_Settings;

        private long m_LastError = (long)SocketError.Success;
        private long m_State = (long)RedisConnectionState.Idle;

        private string m_Name;
        private Action<RedisConnection, RedisSocket> m_CreateAction;
        private Action<RedisConnection, RedisSocket> m_ReleaseAction;

        #endregion Field Members

        #region .Ctors

        internal RedisConnection(string name,
            Action<RedisConnection, RedisSocket> onCreateSocket,
            Action<RedisConnection, RedisSocket> onReleaseSocket,
            RedisSocket socket = null, bool connectImmediately = false)
            : this(name, RedisSettings.Default, onCreateSocket, onReleaseSocket, socket, connectImmediately)
        { }

        internal RedisConnection(string name, RedisSettings settings,
            Action<RedisConnection, RedisSocket> onCreateSocket,
            Action<RedisConnection, RedisSocket> onReleaseSocket,
            RedisSocket socket = null, bool connectImmediately = false)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"));

            if (onReleaseSocket == null)
                throw new RedisFatalException(new ArgumentNullException("releaseAction"));

            m_Settings = settings;
            m_CreateAction = onCreateSocket;
            m_ReleaseAction = onReleaseSocket;
            m_Name = String.IsNullOrEmpty(name) ? Guid.NewGuid().ToString("N") : name;

            if ((socket != null) && socket.Connected)
            {
                m_Socket = socket;
                m_EndPoint = socket.RemoteEndPoint;
                m_State = (int)RedisConnectionState.Connected;
            }

            if (connectImmediately)
                ConnectInternal();
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            Interlocked.Exchange(ref m_Settings, null);
            Interlocked.Exchange(ref m_CreateAction, null);

            var disposeSocket = disposing;
            var socket = Interlocked.Exchange(ref m_Socket, null);
            try
            {
                var onReleaseSocket = Interlocked.Exchange(ref m_ReleaseAction, null);
                if (onReleaseSocket != null)
                    onReleaseSocket(this, socket);
            }
            catch (Exception)
            {
                disposeSocket = true;
            }
            finally
            {
                if (!disposeSocket)
                    socket.DisposeSocket();
            }
        }

        #endregion Destructors

        #region Properties

        public bool Connected
        {
            get
            {
                if (!Disposed)
                    return m_Socket.IsConnected();
                return false;
            }
        }

        public long LastError
        {
            get { return Interlocked.Read(ref m_LastError); }
        }

        public string Name
        {
            get { return m_Name; }
        }

        public RedisSettings Settings
        {
            get { return m_Settings; }
        }

        public RedisConnectionState State
        {
            get { return (RedisConnectionState)m_State; }
        }

        #endregion Properties

        #region Member Methods

        protected RedisSocket GetSocket()
        {
            return m_Socket;
        }

        protected bool Auth(RedisSocket socket, string password)
        {
            if (!String.IsNullOrEmpty(password))
            {
                ValidateNotDisposed();
                using (var cmd = new RedisCommand(-1, RedisCommands.Auth, RedisCommandType.SendAndReceive, password.ToBytes()))
                {
                    return cmd.ExpectSimpleString(new RedisSocketContext(socket, Settings), RedisConstants.OK, true);
                }
            }
            return true;
        }

        protected bool SetClientName(RedisSocket socket, string clientName)
        {
            if (clientName == null)
                throw new ArgumentNullException("clientName");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(-1, RedisCommands.Client, RedisCommandType.SendAndReceive, RedisCommands.SetName, clientName.ToBytes()))
            {
                return cmd.ExpectSimpleString(new RedisSocketContext(socket, Settings), RedisConstants.OK, true);
            }
        }

        public virtual RedisRawResponse SendReceive(byte[] data)
        {
            ValidateNotDisposed();
            throw new RedisFatalException("SendAndReceive is not supported by base connection. Use Send method for sending data.");
        }

        public virtual RedisRawResponse SendReceive(IRedisCommand cmd)
        {
            ValidateNotDisposed();
            throw new RedisFatalException("SendAndReceive is not supported by base connection. Use Send method for sending command.");
        }

        public override void ValidateNotDisposed()
        {
            if (Disposed)
            {
                if (!String.IsNullOrEmpty(Name))
                    throw new RedisFatalException(new ObjectDisposedException(Name));
                base.ValidateNotDisposed();
            }
        }

        protected override bool SetDisposed()
        {
            SetState((long)RedisConnectionState.Disposed);
            return base.SetDisposed();
        }

        public RedisSocket Connect()
        {
            ValidateNotDisposed();
            return ConnectInternal();
        }

        public Task<RedisSocket> ConnectAsync()
        {
            Func<RedisSocket> f = () =>
            {
                return ConnectInternal();
            };
            return f.InvokeAsync();
        }

        protected virtual void OnConnect(RedisSocket socket)
        {
            if (socket.IsConnected())
            {
                var settings = (Settings ?? RedisSettings.Default);

                if (!String.IsNullOrEmpty(settings.Password) &&
                    Auth(socket, settings.Password))
                    socket.SetAuthenticated(true);

                if (!String.IsNullOrEmpty(settings.ClientName))
                    SetClientName(socket, settings.ClientName);
            }
        }

        protected virtual RedisSocket NewSocket()
        {
            var socket = new RedisSocket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp, Settings.UseSsl);

            var onCreateSocket = m_CreateAction;
            if (onCreateSocket != null)
                onCreateSocket(this, socket);

            return socket;
        }

        protected RedisSocket ConnectInternal()
        {
            var socket = m_Socket;
            try
            {
                if ((socket == null) || !socket.Connected)
                {
                    if (socket != null)
                    {
                        Interlocked.Exchange(ref m_Socket, null);
                        socket.DisposeSocket();
                    }

                    SetState((long)RedisConnectionState.Connecting);

                    var task = RedisAsyncEx.GetHostAddressesAsync(m_Settings.Host);

                    socket = NewSocket();
                    ConfigureInternal(socket);

                    var ipAddress = task.Result;
                    if (ipAddress == null)
                        throw new RedisFatalException("Can not resolce host address");

                    socket.ConnectAsync(ipAddress, m_Settings.Port)
                        .ContinueWith(ca =>
                        {
                            if (ca.IsFaulted)
                            {
                                SetState((long)RedisConnectionState.Failed);

                                var exception = ca.Exception as Exception;
                                while (exception != null)
                                {
                                    if (exception is SocketException)
                                        break;
                                    exception = exception.InnerException;
                                }

                                if (exception is SocketException)
                                    SetLastError(((SocketException)exception).ErrorCode);
                            }
                            else if (ca.IsCompleted)
                            {
                                SetState((long)RedisConnectionState.Connected);

                                var prevSocket = Interlocked.Exchange(ref m_Socket, socket);
                                if (prevSocket != null && prevSocket != socket)
                                    prevSocket.DisposeSocket();
                            }
                        }).Wait();

                    if (socket.IsConnected())
                        OnConnect(socket);

                    if (!socket.IsConnected())
                    {
                        socket.DisposeSocket();
                        socket = null;
                    }
                }
            }
            catch (Exception e)
            {
                SetState((long)RedisConnectionState.Failed);

                Interlocked.CompareExchange(ref m_Socket, null, socket);
                socket.DisposeSocket();

                throw new RedisFatalException(e);
            }
            return socket;
        }

        protected long SetState(long state)
        {
            return Interlocked.Exchange(ref m_State, state);
        }

        protected long SetLastError(long error)
        {
            return Interlocked.Exchange(ref m_LastError, error);
        }

        protected void ConfigureInternal(RedisSocket socket)
        {
            DoConfigure(socket);
        }

        protected virtual int GetReceiveTimeout()
        {
            var settings = m_Settings;
            if (settings != null)
            {
                return settings.ReceiveTimeout;
            }
            return RedisConstants.DefaultReceiveTimeout;
        }

        protected virtual int GetSendTimeout()
        {
            var settings = m_Settings;
            if (settings != null)
            {
                return settings.SendTimeout;
            }
            return RedisConstants.DefaultSendTimeout;
        }

        protected virtual void DoConfigure(RedisSocket socket)
        {
            SetIOLoopbackFastPath(socket);

            var sendTimeout = GetSendTimeout();
            var receiveTimeout = GetReceiveTimeout();

            if (sendTimeout > 0)
            {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout,
                                        sendTimeout == int.MaxValue ? Timeout.Infinite : sendTimeout);
            }

            if (receiveTimeout > 0)
            {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout,
                                        receiveTimeout == int.MaxValue ? Timeout.Infinite : receiveTimeout);
            }

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.NoDelay = true;
        }

        protected void SetIOLoopbackFastPath(RedisSocket socket)
        {
            if (RedisCommon.IsWindows)
            {
                var optionInValue = BitConverter.GetBytes(1);
                try
                {
                    socket.IOControl(SIO_LOOPBACK_FAST_PATH, optionInValue, null);
                }
                catch (Exception)
                { }
            }
        }

        public void Send(byte[] data)
        {
            ValidateNotDisposed();

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected));
            }

            socket.SendAsync(data, 0, data.Length).Wait();
        }

        public void Send(IRedisCommand cmd)
        {
            if (cmd == null)
                throw new RedisFatalException(new ArgumentNullException("cmd"));

            ValidateNotDisposed();

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected));
            }

            cmd.WriteTo(socket);
        }

        public Task SendAsync(byte[] data)
        {
            ValidateNotDisposed();

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected));
            }
            return socket.SendAsync(data, 0, data.Length);
        }

        public Task SendAsync(IRedisCommand cmd)
        {
            if (cmd == null)
                throw new RedisFatalException(new ArgumentNullException("cmd"));

            ValidateNotDisposed();

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected));
            }
            return cmd.WriteToAsync(socket);
        }

        #endregion Member Methods
    }
}
