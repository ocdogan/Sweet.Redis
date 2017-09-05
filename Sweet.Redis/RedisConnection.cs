using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisConnection : RedisDisposable, IRedisConnection
    {
        #region AsyncState

        private enum AsyncState : long
        {
            Idle,
            Failed,
            Completed,
            Connecting,
            Sending,
            TimedOut,
            Receiving
        }

        #endregion AsyncState

        #region AsyncUserData

        private class AsyncUserData
        {
            public int Timeout;
            public Socket Socket;
            public long State;
            public ManualResetEvent WaitHandle = new ManualResetEvent(false);
        }

        #endregion AsyncUserData

        #region Constants

        private const int SIO_LOOPBACK_FAST_PATH = -1744830448;

        #endregion Constants

        #region Static Members

        private static readonly ConcurrentQueue<SocketAsyncEventArgs> s_ReceiveEventArgsQ =
            new ConcurrentQueue<SocketAsyncEventArgs>();

        #endregion Static Members

        #region Field Members

        private int m_Db;
        private string m_Name;
        private Socket m_Socket;
        private EndPoint m_EndPoint;
        private RedisSettings m_Settings;

        private long m_LastError = (long)SocketError.Success;
        private long m_State = (long)RedisConnectionState.Idle;
        private Action<RedisConnection, Socket> m_ReleaseAction;

        #endregion Field Members

        #region .Ctors

        internal RedisConnection(RedisConnectionPool pool,
            Action<RedisConnection, Socket> releaseAction, int db, Socket socket = null,
            bool connectImmediately = false)
            : this(pool, new RedisSettings(), releaseAction, db, socket, connectImmediately)
        { }

        internal RedisConnection(RedisConnectionPool pool, RedisSettings settings,
            Action<RedisConnection, Socket> releaseAction, int db, Socket socket = null,
            bool connectImmediately = false)
        {
            if (pool == null)
                throw new ArgumentNullException("pool");

            if (settings == null)
                throw new ArgumentNullException("settings");

            if (releaseAction == null)
                throw new ArgumentNullException("releaseAction");

            m_Db = db;
            m_Name = pool.Name;
            m_Settings = settings;
            m_ReleaseAction = releaseAction;

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
            Interlocked.Exchange(ref m_Settings, null);

            var socket = Interlocked.Exchange(ref m_Socket, null);
            if (socket == null)
                return;

            if (disposing)
            {
                if (m_ReleaseAction != null)
                {
                    m_ReleaseAction(this, socket);
                }
            }
            else if (!socket.Connected)
            {
                socket.Dispose();
            }
            else
            {
                var userToken = new AsyncUserData
                {
                    Socket = socket
                };

                var args = new SocketAsyncEventArgs
                {
                    RemoteEndPoint = m_EndPoint,
                    UserToken = userToken,
                };

                args.Completed += (sender, e) =>
                {
                    var token = (AsyncUserData)e.UserToken;
                    if (token.Socket != null)
                    {
                        token.Socket.Dispose();
                    }
                };

                socket.DisconnectAsync(args);
            }
        }

        #endregion Destructors

        #region Properties

        public int Db
        {
            get { return m_Db; }
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

        protected override void ValidateNotDisposed()
        {
            if (Disposed)
            {
                if (!String.IsNullOrEmpty(Name))
                    throw new ObjectDisposedException(Name);
                base.ValidateNotDisposed();
            }
        }

        protected override bool SetDisposed()
        {
            SetState((long)RedisConnectionState.Disposed);
            return base.SetDisposed();
        }

        public bool Available()
        {
            var socket = ConnectInternal();
            return socket != null && socket.Connected && (socket.Available > 0);
        }

        internal Socket Connect()
        {
            return ConnectInternal();
        }

        internal Socket ConnectInternal()
        {
            var socket = m_Socket;
            var errorOccured = false;
            try
            {
                if ((socket == null) || !socket.Connected)
                {
                    if (socket != null)
                    {
                        Interlocked.Exchange(ref m_Socket, null);
                        socket.Dispose();
                    }

                    SetState((long)RedisConnectionState.Connecting);

                    var endPoint = GetEndPoint();
                    Interlocked.Exchange(ref m_EndPoint, endPoint);

                    socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    ConfigureInternal(socket);

                    var userToken = new AsyncUserData
                    {
                        Socket = socket,
                        State = (long)AsyncState.Connecting,
                        Timeout = m_Settings.ConnectionTimeout
                    };

                    var args = new SocketAsyncEventArgs
                    {
                        RemoteEndPoint = endPoint,
                        UserToken = userToken,
                    };
                    args.Completed += OnConnect;

                    // On connection timeout
                    if (socket.ConnectAsync(args) &&
                        !userToken.WaitHandle.WaitOne(userToken.Timeout) && !socket.Connected)
                    {
                        Interlocked.Exchange(ref userToken.State, (long)AsyncState.TimedOut);

                        SetLastError((long)SocketError.TimedOut);
                        SetState((long)RedisConnectionState.Failed);

                        args.Dispose();
                        socket.Dispose();

                        throw new SocketException((int)SocketError.TimedOut);
                    }

                    // Has error or not connected
                    if ((args.SocketError != SocketError.Success) || !socket.Connected)
                    {
                        Interlocked.Exchange(ref userToken.State, (long)AsyncState.Failed);

                        SetLastError((long)args.SocketError);
                        SetState((long)RedisConnectionState.Failed);

                        args.Dispose();
                        socket.Dispose();

                        throw new SocketException((int)args.SocketError);
                    }
                }
            }
            catch (Exception)
            {
                errorOccured = true;
                throw;
            }
            finally
            {
                if (errorOccured)
                {
                    Interlocked.CompareExchange(ref m_Socket, null, socket);
                    Close(socket);
                }
                else
                {
                    var prevSocket = Interlocked.Exchange(ref m_Socket, socket);
                    if (prevSocket != socket)
                        Close(prevSocket);
                }
            }
            return socket;
        }

        private void Close(Socket socket)
        {
            if (socket != null)
            {
                Task.Factory.StartNew(obj =>
                {
                    var sock = obj as Socket;
                    if (sock != null)
                    {
                        try
                        {
                            socket.Dispose();
                        }
                        catch (Exception)
                        { }
                    }
                }, socket);
            }
        }

        internal long SetState(long state)
        {
            return Interlocked.Exchange(ref m_State, state);
        }

        internal long SetLastError(long error)
        {
            return Interlocked.Exchange(ref m_LastError, error);
        }

        private void OnConnect(object sender, SocketAsyncEventArgs e)
        {
            // Set last error
            SetLastError((long)e.SocketError);

            // Set current state
            Interlocked.Exchange(ref m_State, (e.SocketError == SocketError.Success) ?
                (int)RedisConnectionState.Connected : (int)RedisConnectionState.Failed);

            var userToken = (AsyncUserData)e.UserToken;
            if (e.SocketError != SocketError.Success)
                Interlocked.Exchange(ref userToken.State, (long)AsyncState.Failed);

            // Signals the end of connection.
            userToken.WaitHandle.Set();
        }

        protected virtual void DoConnected(object sender, SocketAsyncEventArgs e)
        { }

        protected void ConfigureInternal(Socket socket)
        {
            DoConfigure(socket);
        }

        private EndPoint GetEndPoint()
        {
            var host = Dns.GetHostEntry(m_Settings.Host);

            var addressList = host.AddressList;
            return new IPEndPoint(addressList[addressList.Length - 1], m_Settings.Port);
        }

        protected virtual void DoConfigure(Socket socket)
        {
            SetIOLoopbackFastPath(socket);

            socket.Blocking = false;

            var settings = m_Settings;
            if (settings != null)
            {
                if (settings.SendTimeout > 0)
                {
                    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout,
                                           settings.SendTimeout == int.MaxValue ? Timeout.Infinite : settings.SendTimeout);
                }

                if (settings.ReceiveTimeout > 0)
                {
                    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout,
                                           settings.ReceiveTimeout == int.MaxValue ? Timeout.Infinite : settings.ReceiveTimeout);
                }
            }
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.NoDelay = true;
        }

        protected void SetIOLoopbackFastPath(Socket socket)
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

        public IRedisResponse Send(byte[] data)
        {
            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new SocketException((int)SocketError.NotConnected);
            }

            var args = NewSendArgs(data, socket);
            var userToken = (AsyncUserData)args.UserToken;

            // On send timeout
            if (socket.SendAsync(args) &&
                !userToken.WaitHandle.WaitOne(userToken.Timeout))
            {
                Interlocked.Exchange(ref userToken.State, (long)AsyncState.TimedOut);

                SetLastError((long)SocketError.TimedOut);
                SetState((long)RedisConnectionState.Failed);

                ReleaseSendEventArgs(args);

                throw new SocketException((int)SocketError.TimedOut);
            }

            // Has error
            if (args.SocketError != SocketError.Success)
            {
                Interlocked.Exchange(ref userToken.State, (long)AsyncState.Failed);

                SetLastError((long)args.SocketError);
                SetState((long)RedisConnectionState.Failed);

                ReleaseSendEventArgs(args);
                if (args.SocketError == SocketError.OperationAborted)
                {
                    Interlocked.CompareExchange(ref m_Socket, null, socket);
                    Close(socket);
                }

                throw new SocketException((int)args.SocketError);
            }

            ReleaseSendEventArgs(args);

            using (var reader = new RedisResponseReader(this))
            {
                return reader.Execute();
            }
        }

        private void ReleaseSendEventArgs(SocketAsyncEventArgs args)
        {
            args.Completed -= OnSend;
            args.Dispose();
        }

        private void OnSend(object sender, SocketAsyncEventArgs e)
        {
            e.Completed -= OnSend;

            // Set last error
            SetLastError((long)e.SocketError);

            var userToken = (AsyncUserData)e.UserToken;
            if (e.SocketError != SocketError.Success)
                Interlocked.Exchange(ref userToken.State, (long)AsyncState.Failed);

            // Signals the end of send.
            userToken.WaitHandle.Set();
        }

        private SocketAsyncEventArgs NewSendArgs(byte[] data, Socket socket)
        {
            var userToken = new AsyncUserData
            {
                Socket = socket,
                State = (long)AsyncState.Sending,
                Timeout = m_Settings.SendTimeout
            };

            var args = new SocketAsyncEventArgs
            {
                UserToken = userToken,
            };

            args.SetBuffer(data, 0, data != null ? data.Length : 0);
            args.Completed += OnSend;

            return args;
        }

        internal RedisReceivedData Receive()
        {
            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new SocketException((int)SocketError.NotConnected);
            }

            var args = NewReceiveArgs(socket);

            var userToken = args.UserToken as AsyncUserData;
            userToken.Socket = socket;

            var now = DateTime.UtcNow;

            // On receive timeout
            if (socket.ReceiveAsync(args) &&
                !userToken.WaitHandle.WaitOne(userToken.Timeout))
            {
                Interlocked.Exchange(ref userToken.State, (long)AsyncState.TimedOut);

                SetLastError((long)SocketError.TimedOut);
                SetState((long)RedisConnectionState.Failed);

                ReleaseReceiveEventArgs(args);

                throw new SocketException((int)SocketError.TimedOut);
            }

            // Has error
            if (args.SocketError != SocketError.Success)
            {
                Interlocked.Exchange(ref userToken.State, (long)AsyncState.Failed);

                SetLastError((long)args.SocketError);
                SetState((long)RedisConnectionState.Failed);

                ReleaseReceiveEventArgs(args);
                if (args.SocketError == SocketError.OperationAborted)
                {
                    Interlocked.CompareExchange(ref m_Socket, null, socket);
                    Close(socket);
                }

                throw new SocketException((int)args.SocketError);
            }

            var receivedLength = Math.Max(0, args.BytesTransferred);
            if (receivedLength == 0)
                return RedisReceivedData.Empty;

            var state = (AsyncState)userToken.State;
            if (state != AsyncState.Receiving)
                return RedisReceivedData.Empty;

            if (userToken.Timeout > -1)
            {
                var ellapsed = (DateTime.UtcNow - now).TotalMilliseconds;

                userToken.Timeout -= ellapsed >= userToken.Timeout ?
                    userToken.Timeout : (int)ellapsed;

                if (userToken.Timeout <= 0)
                {
                    userToken.Timeout = 0;

                    SetLastError((long)SocketError.TimedOut);
                    SetState((long)RedisConnectionState.Failed);

                    ReleaseReceiveEventArgs(args);

                    throw new SocketException((int)SocketError.TimedOut);
                }
            }

            byte[] data = null;
            var buffer = args.Buffer;
            if (args.Offset == 0 && receivedLength == buffer.Length)
            {
                data = buffer;
                args.SetBuffer(new byte[RedisConstants.DefaultBufferSize], 0, RedisConstants.DefaultBufferSize);
            }
            else
            {
                data = new byte[receivedLength];
                if (receivedLength > 0)
                    Buffer.BlockCopy(buffer, args.Offset, data, 0, receivedLength);
            }

            ReleaseReceiveEventArgs(args);

            return new RedisReceivedData(data: data, available: socket.Available);
        }

        private void ReleaseReceiveEventArgs(SocketAsyncEventArgs args)
        {
            if (args != null)
            {
                args.Completed -= OnReceive;

                var userToken = (AsyncUserData)args.UserToken;
                userToken.Socket = null;

                userToken.WaitHandle.Reset();

                s_ReceiveEventArgsQ.Enqueue(args);
            }
        }

        private SocketAsyncEventArgs NewReceiveArgs(Socket socket)
        {
            SocketAsyncEventArgs args;
            s_ReceiveEventArgsQ.TryDequeue(out args);

            AsyncUserData userToken;
            if (args != null)
            {
                userToken = (AsyncUserData)args.UserToken;

                userToken.Socket = socket;
                userToken.State = (long)AsyncState.Receiving;
                userToken.Timeout = m_Settings.ReceiveTimeout;
            }
            else
            {
                userToken = new AsyncUserData
                {
                    Socket = socket,
                    State = (long)AsyncState.Receiving,
                    Timeout = m_Settings.ReceiveTimeout
                };

                args = new SocketAsyncEventArgs
                {
                    UserToken = userToken,
                };

                args.SetBuffer(new byte[RedisConstants.DefaultBufferSize], 0, RedisConstants.DefaultBufferSize);
            }

            args.Completed += OnReceive;
            return args;
        }

        private void OnReceive(object sender, SocketAsyncEventArgs e)
        {
            e.Completed -= OnReceive;

            // Set last error
            SetLastError((long)e.SocketError);

            var userToken = (AsyncUserData)e.UserToken;
            if (e.SocketError != SocketError.Success)
                Interlocked.Exchange(ref userToken.State, (long)AsyncState.Failed);

            // Signals the end of receive.
            userToken.WaitHandle.Set();
        }

        #endregion Member Methods
    }
}
