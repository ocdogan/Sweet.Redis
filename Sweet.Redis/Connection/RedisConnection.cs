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
    internal abstract class RedisConnection : RedisDisposable, IRedisConnection, IRedisNamedObject, IRedisIdentifiedObject
    {
        #region Constants

        private const int SIO_LOOPBACK_FAST_PATH = -1744830448;

        #endregion Constants

        #region Field Members

        private long m_Id;
        private string m_Name;

        private RedisRole m_ExpectedRole = RedisRole.Undefined;

        protected RedisSocket m_Socket;
        protected RedisConnectionSettings m_Settings;

        private long m_LastError = (long)SocketError.Success;
        private long m_State = (long)RedisConnectionState.Idle;

        protected Action<RedisConnection, RedisSocket> m_CreateAction;
        protected Action<RedisConnection, RedisSocket> m_ReleaseAction;

        #endregion Field Members

        #region .Ctors

        internal RedisConnection(string name,
            RedisRole expectedRole, RedisConnectionSettings settings,
            Action<RedisConnection, RedisSocket> onCreateSocket,
            Action<RedisConnection, RedisSocket> onReleaseSocket,
            RedisSocket socket = null, bool connectImmediately = false)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"));

            if (onReleaseSocket == null)
                throw new RedisFatalException(new ArgumentNullException("onReleaseSocket"));

            m_Id = RedisIDGenerator<RedisConnection>.NextId();

            m_ExpectedRole = expectedRole;
            m_Settings = settings ?? RedisConnectionSettings.Default;
            m_CreateAction = onCreateSocket;
            m_ReleaseAction = onReleaseSocket;
            m_Name = !name.IsEmpty() ? name : (GetType().Name + ", " + m_Id.ToString());

            if ((socket != null) && socket.Connected)
            {
                m_Socket = socket;
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

            var onReleaseSocket = Interlocked.Exchange(ref m_ReleaseAction, null);
            if (onReleaseSocket != null)
            {
                var socket = Interlocked.Exchange(ref m_Socket, null);
                try
                {
                    if (onReleaseSocket != null)
                        onReleaseSocket(this, socket);
                }
                catch (Exception e)
                {
                    if (e.IsSocketError())
                        socket.DisposeSocket();
                }
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

        public RedisRole ExpectedRole
        {
            get { return m_ExpectedRole; }
        }

        public long Id
        {
            get { return m_Id; }
        }

        public long LastError
        {
            get { return Interlocked.Read(ref m_LastError); }
        }

        public string Name
        {
            get { return m_Name; }
        }

        public RedisRole ServerRole
        {
            get
            {
                var socket = m_Socket;
                return socket != null ? socket.Role : RedisRole.Undefined;
            }
        }

        public RedisConnectionSettings Settings
        {
            get { return m_Settings; }
        }

        public RedisConnectionState State
        {
            get { return (RedisConnectionState)m_State; }
        }

        #endregion Properties

        #region Methods

        protected RedisSocket GetSocket()
        {
            return m_Socket;
        }

        void IRedisConnection.FreeAndNilSocket()
        {
            this.FreeAndNilSocket();
        }

        protected void FreeAndNilSocket()
        {
            var socket = Interlocked.Exchange(ref m_Socket, null);
            if (socket != null)
                socket.DisposeSocket();
        }

        protected bool Auth(RedisSocket socket, string password)
        {
            if (!password.IsEmpty())
            {
                ValidateNotDisposed();
                using (var cmd = new RedisCommand(-1, RedisCommandList.Auth, RedisCommandType.SendAndReceive, password.ToBytes()))
                {
                    return cmd.ExpectOK(new RedisSocketContext(socket, Settings), true);
                }
            }
            return true;
        }

        protected bool SetClientName(RedisSocket socket, string clientName)
        {
            if (clientName == null)
                throw new ArgumentNullException("clientName");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(-1, RedisCommandList.Client, RedisCommandType.SendAndReceive, RedisCommandList.SetName, clientName.ToBytes()))
            {
                return cmd.ExpectOK(new RedisSocketContext(socket, Settings), true);
            }
        }

        protected virtual bool NeedsToDiscoverRole()
        {
            return !(m_ExpectedRole == RedisRole.Any ||
                     m_ExpectedRole == RedisRole.Undefined);
        }

        protected virtual void ValidateRole(RedisRole commandRole)
        {
            if (!(commandRole == RedisRole.Undefined || commandRole == RedisRole.Any))
            {
                var serverRole = ServerRole;
                if (serverRole != RedisRole.Any && serverRole != commandRole &&
                    (serverRole == RedisRole.Sentinel || commandRole == RedisRole.Sentinel ||
                     (serverRole == RedisRole.Slave && commandRole == RedisRole.Master)))
                    throw new RedisException("Connected server does not satisfy the command's desired role", RedisErrorCode.NotSupported);
            }
        }

        public RedisServerInfo GetServerInfo()
        {
            try
            {
                var socket = GetSocket();
                if (socket.IsConnected())
                {
                    using (var cmd = new RedisCommand(-1, RedisCommandList.Info, RedisCommandType.SendAndReceive))
                    {
                        string lines = cmd.ExpectBulkString(new RedisSocketContext(socket, Settings), true);
                        return RedisServerInfo.Parse(lines);
                    }
                }
            }
            catch (Exception)
            { }
            return null;
        }

        protected virtual RedisRole DiscoverRole(RedisSocket socket)
        {
            if (!Disposed)
            {
                var role = RedisRole.Undefined;
                try
                {
                    using (var cmd = new RedisCommand(-1, RedisCommandList.Role, RedisCommandType.SendAndReceive))
                    {
                        var raw = cmd.ExpectArray(new RedisSocketContext(socket, Settings), true);
                        if (!ReferenceEquals(raw, null) && raw.IsCompleted)
                        {
                            var info = RedisRoleInfo.Parse(raw.Value);
                            if (!ReferenceEquals(info, null))
                                role = info.Role;
                        }
                    }
                }
                catch (Exception e)
                {
                    var exception = e;
                    while (exception != null)
                    {
                        if (exception is SocketException)
                            return RedisRole.Undefined;

                        var re = e as RedisException;
                        if (re != null && (re.ErrorCode == RedisErrorCode.ConnectionError ||
                            re.ErrorCode == RedisErrorCode.SocketError))
                            return RedisRole.Undefined;

                        exception = exception.InnerException;
                    }
                }

                if (role == RedisRole.Undefined)
                {
                    try
                    {
                        var serverInfo = GetServerInfo();
                        if (serverInfo != null)
                        {
                            var serverSection = serverInfo.Server;
                            if (serverSection != null)
                                role = (serverSection.RedisMode ?? String.Empty).Trim().ToRedisRole();

                            if (role == RedisRole.Undefined)
                            {
                                var replicationSection = serverInfo.Replication;
                                if (replicationSection != null)
                                    role = (replicationSection.Role ?? String.Empty).Trim().ToRedisRole();

                                if (role == RedisRole.Undefined)
                                {
                                    var sentinelSection = serverInfo.Sentinel;
                                    if (sentinelSection != null && sentinelSection.SentinelMasters.HasValue)
                                        role = RedisRole.Sentinel;

                                    if (role == RedisRole.Undefined)
                                        role = RedisRole.Master;
                                }
                            }
                        }
                    }
                    catch (Exception)
                    { }
                }

                socket.Role = role;
                return role;
            }
            return RedisRole.Undefined;
        }

        public override void ValidateNotDisposed()
        {
            if (Disposed)
            {
                if (!Name.IsEmpty())
                    throw new RedisFatalException(new ObjectDisposedException(Name), RedisErrorCode.ObjectDisposed);
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
            Func<RedisSocket> f = ConnectInternal;
            return f.InvokeAsync();
        }

        protected virtual void OnConnect(RedisSocket socket)
        {
            if (socket.IsConnected())
            {
                var settings = (Settings ?? RedisPoolSettings.Default);

                if (!settings.Password.IsEmpty() &&
                    Auth(socket, settings.Password))
                    socket.SetAuthenticated(true);

                if (!settings.ClientName.IsEmpty())
                    SetClientName(socket, settings.ClientName);

                if (!NeedsToDiscoverRole())
                    socket.Role = ExpectedRole;
                else
                {
                    var role = DiscoverRole(socket);
                    ValidateRole(role);
                }
            }
        }

        protected virtual RedisSocket NewSocket(IPAddress ipAddress)
        {
            var socket = new RedisSocket(ipAddress != null ? ipAddress.AddressFamily : AddressFamily.InterNetwork,
                SocketType.Stream, ProtocolType.Tcp, Settings.UseSsl);

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
                        Interlocked.CompareExchange(ref m_Socket, null, socket);
                        socket.DisposeSocket();
                    }

                    SetState((long)RedisConnectionState.Connecting);

                    RedisEndPoint endPoint;
                    socket = CreateSocket(out endPoint);

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

                throw new RedisFatalException(e, RedisErrorCode.ConnectionError);
            }
            return socket;
        }

        private RedisSocket CreateSocket(out RedisEndPoint endPoint)
        {
            endPoint = null;

            var endPoints = m_Settings.EndPoints;
            if (endPoints != null)
            {
                foreach (var ep in endPoints)
                {
                    if (!ep.IsEmpty())
                    {
                        try
                        {
                            var ipAddresses = ep.ResolveHost();
                            if (ipAddresses != null)
                            {
                                var length = ipAddresses.Length;
                                if (length > 0)
                                {
                                    for (var i = 0; i < length; i++)
                                    {
                                        try
                                        {
                                            var socket = CreateSocket(ep, ipAddresses[i]);
                                            if (socket.IsConnected())
                                            {
                                                endPoint = ep;
                                                return socket;
                                            }
                                        }
                                        catch (Exception)
                                        { }
                                    }
                                }
                            }
                        }
                        catch (Exception)
                        { }
                    }
                }
            }
            throw new RedisFatalException("Can not resolve end-point address", RedisErrorCode.ConnectionError);
        }

        private RedisSocket CreateSocket(RedisEndPoint endPoint, IPAddress ipAddress)
        {
            var socket = NewSocket(ipAddress);
            ConfigureInternal(socket);

            socket.ConnectAsync(ipAddress, endPoint.Port)
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

                        var oldSocket = Interlocked.Exchange(ref m_Socket, socket);
                        if (oldSocket != null && oldSocket != socket)
                            oldSocket.DisposeSocket();
                    }
                }).Wait();
            return socket;
        }

        public RedisSocket RemoveSocket()
        {
            return Interlocked.Exchange(ref m_Socket, null);
        }

        public virtual void ReleaseSocket()
        {
            try
            {
                var socket = Interlocked.Exchange(ref m_Socket, null);
                if (socket != null)
                {
                    var onReleaseSocket = m_ReleaseAction;
                    if (onReleaseSocket != null)
                        onReleaseSocket(this, socket);
                }
            }
            catch (Exception)
            { }
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

        public virtual RedisRawResponse SendReceive(byte[] data, RedisRole commandRole)
        {
            ValidateNotDisposed();
            throw new RedisFatalException("SendAndReceive is not supported by base connection. Use Send method for sending data.", RedisErrorCode.NotSupported);
        }

        public virtual RedisRawResponse SendReceive(IRedisCommand cmd)
        {
            ValidateNotDisposed();
            throw new RedisFatalException("SendAndReceive is not supported by base connection. Use Send method for sending command.", RedisErrorCode.NotSupported);
        }

        public void Send(byte[] data, RedisRole commandRole)
        {
            ValidateNotDisposed();
            ValidateRole(commandRole);

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected));
            }

            var task = socket.SendAsync(data, 0, data.Length);
            task.ContinueWith((asyncTask) =>
            {
                if (asyncTask.IsFaulted && asyncTask.Exception.IsSocketError())
                    FreeAndNilSocket();
            });
            task.Wait();
        }

        public void Send(IRedisCommand cmd)
        {
            if (cmd == null)
                throw new RedisFatalException(new ArgumentNullException("cmd"), RedisErrorCode.MissingParameter);

            ValidateNotDisposed();
            ValidateRole(cmd.Role);

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected), RedisErrorCode.ConnectionError);
            }

            try
            {
                cmd.WriteTo(socket);
            }
            catch (Exception e)
            {
                if (e.IsSocketError())
                    FreeAndNilSocket();
                throw;
            }
        }

        public Task SendAsync(byte[] data, RedisRole commandRole)
        {
            ValidateNotDisposed();
            ValidateRole(commandRole);

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected), RedisErrorCode.ConnectionError);
            }

            var task = socket.SendAsync(data, 0, data.Length);
            task.ContinueWith((asyncTask) =>
            {
                if (asyncTask.IsFaulted && asyncTask.Exception.IsSocketError())
                    FreeAndNilSocket();
            });
            return task;
        }

        public Task SendAsync(IRedisCommand cmd)
        {
            if (cmd == null)
                throw new RedisFatalException(new ArgumentNullException("cmd"), RedisErrorCode.MissingParameter);

            ValidateNotDisposed();
            ValidateRole(cmd.Role);

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected));
            }

            var task = cmd.WriteToAsync(socket);
            task.ContinueWith((asyncTask) =>
            {
                if (asyncTask.IsFaulted && asyncTask.Exception.IsSocketError())
                    FreeAndNilSocket();
            });
            return task;
        }

        #endregion Methods
    }
}
