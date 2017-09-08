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
        #region Constants

        private const int SIO_LOOPBACK_FAST_PATH = -1744830448;

        #endregion Constants

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

            if (!disposing)
                socket.DisposeSocket();
            else if (m_ReleaseAction != null)
                m_ReleaseAction(this, socket);
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

        internal Socket Connect()
        {
            ValidateNotDisposed();
            return ConnectInternal();
        }

        private Socket ConnectInternal()
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

                        return null;
                    }

                    SetState((long)RedisConnectionState.Connecting);

                    var task = RedisAsyncEx.GetHostAddressesAsync(m_Settings.Host);

                    socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    ConfigureInternal(socket);

                    var ipAddress = task.Result;
                    if (ipAddress == null)
                        throw new RedisException("Can not resolce host address");

                    socket.ConnectAsync(ipAddress, m_Settings.Port).Wait();

                    SetState((long)RedisConnectionState.Connected);

                    var prevSocket = Interlocked.Exchange(ref m_Socket, socket);
                    if (prevSocket != socket)
                        prevSocket.DisposeSocket();
                }
            }
            catch (Exception)
            {
                SetState((long)RedisConnectionState.Failed);

                Interlocked.CompareExchange(ref m_Socket, null, socket);
                socket.DisposeSocket();

                throw;
            }
            return socket;
        }

        internal long SetState(long state)
        {
            return Interlocked.Exchange(ref m_State, state);
        }

        internal long SetLastError(long error)
        {
            return Interlocked.Exchange(ref m_LastError, error);
        }

        protected void ConfigureInternal(Socket socket)
        {
            DoConfigure(socket);
        }

        protected virtual void DoConfigure(Socket socket)
        {
            SetIOLoopbackFastPath(socket);

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
            ValidateNotDisposed();

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new SocketException((int)SocketError.NotConnected);
            }

            var task = socket.SendAsync(data, 0, data.Length)
                .ContinueWith<IRedisResponse>((ret) =>
                {
                    if (ret.IsCompleted && ret.Result > 0)
                        using (var reader = new RedisResponseReader())
                            return reader.Execute(socket);
                    return null;
                });
            return task.Result;
        }

        #endregion Member Methods
    }
}
