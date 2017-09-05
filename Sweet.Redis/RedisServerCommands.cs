using System;
using System.Collections.Generic;

namespace Sweet.Redis
{
    internal class RedisServerCommands : RedisCommandSet, IRedisServerCommands
    {
        #region .Ctors

        public RedisServerCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public bool BGRewriteAOF()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.BGRewriteAOF))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool BGSave()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.BGSave))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public string ClientGetName()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.GetName))
            {
                return cmd.ExpectBulkString(Db.Pool, true);
            }
        }

        public long ClientKill(string ip = null, int port = -1, string clientId = null, string type = null, bool skipMe = true)
        {
            ValidateNotDisposed();

            var parameters = new byte[0].Merge(RedisCommands.Kill);

            if (!String.IsNullOrEmpty(ip))
            {
                parameters = parameters
                    .Merge(RedisCommands.Addr)
                    .Merge(ip.ToBytes());

                if (port > -1)
                    parameters = parameters.Merge(port.ToBytes());
            }

            if (!String.IsNullOrEmpty(clientId))
            {
                parameters = parameters
                    .Merge(RedisCommands.Id)
                    .Merge(clientId.ToBytes());
            }

            if (!String.IsNullOrEmpty(type))
            {
                parameters = parameters
                    .Merge(RedisCommands.Type)
                    .Merge(type.ToBytes());
            }

            if (!skipMe)
            {
                parameters = parameters
                    .Merge(RedisCommands.SkipMe)
                    .Merge(RedisCommands.No);
            }

            using (var cmd = new RedisCommand(RedisCommands.Client, parameters))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public RedisClientInfo[] ClientList()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.List))
            {
                var response = cmd.ExpectBulkString(Db.Pool, true);
                if (response != null)
                {
                    var lines = response.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    if (lines.Length > 0)
                    {
                        var list = new List<RedisClientInfo>(lines.Length);
                        foreach (var line in lines)
                        {
                            var info = RedisClientInfo.Parse(line);
                            if (info != null)
                                list.Add(info);
                        }
                        return (list.Count > 0) ? list.ToArray() : null;
                    }
                }
            }
            return null;
        }

        public IDictionary<string, string>[] ClientListDictionary()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.List))
            {
                var response = cmd.ExpectBulkString(Db.Pool, true);
                if (response != null)
                {
                    var lines = response.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    if (lines.Length > 0)
                    {
                        var list = new List<IDictionary<string, string>>(lines.Length);
                        foreach (var line in lines)
                        {
                            var info = RedisClientInfo.ParseDictionary(line);
                            if (info != null)
                                list.Add(info);
                        }
                        return (list.Count > 0) ? list.ToArray() : null;
                    }
                }
            }
            return null;
        }

        public bool ClientPause(int timeout)
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.Pause, timeout.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool ClientReplyOff()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.Reply, RedisCommands.Off))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool ClientReplyOn()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.Reply, RedisCommands.On))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool ClientReplySkip()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.Reply, RedisCommands.Skip))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool ClientSetName(string connectionName)
        {
            if (connectionName == null)
                throw new ArgumentNullException("connectionName");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Client, RedisCommands.SetName, connectionName.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public IDictionary<string, string> ConfigGet(string parameter)
        {
            if (parameter == null)
                throw new ArgumentNullException("parameter");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Config, RedisCommands.Get, parameter.ToBytes()))
            {
                var lines = cmd.ExpectMultiDataStrings(Db.Pool, true);
                if (lines != null)
                {
                    var length = lines.Length;
                    if (lines.Length > 0)
                    {
                        var result = new Dictionary<string, string>(lines.Length / 2);
                        for (var i = 0; i < length; i += 2)
                        {
                            var key = (lines[i] ?? String.Empty).Trim();
                            if (!String.IsNullOrEmpty(key))
                                result[key] = (lines[i + 1] ?? String.Empty).Trim();
                        }
                        return result;
                    }
                }
            }
            return null;
        }

        public bool ConfigResetStat()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Config, RedisCommands.ResetStat))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool ConfigRewrite()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Config, RedisCommands.Rewrite))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool ConfigSet(string parameter, string value)
        {
            if (parameter == null)
                throw new ArgumentNullException("parameter");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            using (var cmd = new RedisCommand(RedisCommands.Config, RedisCommands.Set, parameter.ToBytes(), bytes))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public long DbSize()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.DbSize))
            {
                return cmd.ExpectInteger(Db.Pool, true);
            }
        }

        public bool FlushAll()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.FlushAll))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool FlushAllAsync()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.FlushAll, RedisCommands.Async))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool FlushDb()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.FlushDb))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool FlushDbAsync()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.FlushDb, RedisCommands.Async))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public string[] Info(string section)
        {
            if (section == null)
                throw new ArgumentNullException("section");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Info, section.ToBytes()))
            {
                return cmd.ExpectMultiDataStrings(Db.Pool, true);
            }
        }

        public DateTime LastSave()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.LastSave))
            {
                return cmd.ExpectInteger(Db.Pool, true).FromUnixTimeStamp();
            }
        }

        public bool Save()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Save))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public void ShutDown()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.ShutDown))
            {
                cmd.ExpectSimpleString(Db.Pool, true);
            }
        }

        public void ShutDownSave()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.ShutDown, RedisCommands.Async))
            {
                cmd.ExpectSimpleString(Db.Pool, true);
            }
        }

        public bool SlaveOf(string host, int port)
        {
            if (host == null)
                throw new ArgumentNullException("host");

            if (port < 0 || port > ushort.MaxValue)
                throw new ArgumentException("Invalid port number");

            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.SlaveOf, host.ToBytes(), port.ToBytes()))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public bool SlaveOfNoOne()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.SlaveOf, RedisCommands.NoOne))
            {
                return cmd.ExpectSimpleString(Db.Pool, "OK", true);
            }
        }

        public DateTime Time()
        {
            ValidateNotDisposed();
            using (var cmd = new RedisCommand(RedisCommands.Time))
            {
                var parts = cmd.ExpectMultiDataStrings(Db.Pool, true);
                if (parts != null && parts.Length > 0)
                {
                    if (parts.Length > 1)
                        return parts[0].ToInt().FromUnixTimeStamp(parts[1].ToInt());
                    return parts[0].ToInt().FromUnixTimeStamp();
                }
            }
            return DateTime.MinValue;
        }

        #endregion Methods
    }
}
