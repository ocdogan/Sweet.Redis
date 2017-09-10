using System;
using System.Collections.Generic;

namespace Sweet.Redis
{
    internal class RedisScriptingCommands : RedisCommandSet, IRedisScriptingCommands
    {
        #region .Ctors

        public RedisScriptingCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        private RedisObject Eval(byte[] cmd, string source, params RedisKeyValue<string, string>[] args)
        {
            var argsLength = args.Length;
            if (argsLength == 0)
                return ExpectArray(cmd, source.ToBytes(), RedisConstants.Zero);

            var parameters = new byte[2 * (1 + argsLength)][];

            parameters[0] = source.ToBytes();
            parameters[1] = argsLength.ToBytes();

            for (int i = 0, paramsIndex = 2; i < argsLength; i++, paramsIndex++)
            {
                parameters[paramsIndex] = args[i].Key.ToBytes();
                parameters[argsLength + paramsIndex] = args[i].Value.ToBytes();
            }

            return ExpectArray(cmd, parameters);
        }

        private RedisObject Eval(byte[] cmd, string source, params RedisKeyValue<string, byte[]>[] args)
        {
            var argsLength = args.Length;
            if (argsLength == 0)
                return ExpectArray(cmd, source.ToBytes(), RedisConstants.Zero);

            var parameters = new byte[2 * (1 + argsLength)][];

            parameters[0] = source.ToBytes();
            parameters[1] = argsLength.ToBytes();

            for (int i = 0, paramsIndex = 2; i < argsLength; i++, paramsIndex++)
            {
                parameters[paramsIndex] = args[i].Key.ToBytes();
                parameters[argsLength + paramsIndex] = args[i].Value.ToBytes();
            }

            return ExpectArray(cmd, parameters);
        }

        public RedisObject Eval(string script, params RedisKeyValue<string, byte[]>[] args)
        {
            if (String.IsNullOrEmpty(script))
                throw new ArgumentNullException("script");

            return Eval(RedisCommands.Eval, script, args);
        }

        public RedisObject EvalString(string script, params RedisKeyValue<string, string>[] args)
        {
            if (String.IsNullOrEmpty(script))
                throw new ArgumentNullException("script");

            return Eval(RedisCommands.Eval, script, args);
        }

        public RedisObject EvalSHA(string sha1, params RedisKeyValue<string, byte[]>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            return Eval(RedisCommands.EvalSha, sha1, args);
        }

        public RedisObject EvalSHA(ref string sha1, string script, params RedisKeyValue<string, byte[]>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            if (String.IsNullOrWhiteSpace(script))
                return Eval(RedisCommands.EvalSha, sha1, args);

            var response = ScriptExists(sha1);
            var exists = (response != null && response.Length > 0) ? response[0] == 1L : false;

            if (!exists)
                sha1 = ScriptLoad(script);

            if (String.IsNullOrEmpty(sha1))
                return null;

            try
            {
                return Eval(RedisCommands.EvalSha, sha1, args);
            }
            catch (RedisException e)
            {
                var msg = e.Message;
                if (!String.IsNullOrEmpty(msg) &&
                    msg.StartsWith("NOSCRIPT", StringComparison.OrdinalIgnoreCase))
                {
                    sha1 = ScriptLoad(script);
                    if (!String.IsNullOrEmpty(sha1))
                        return Eval(RedisCommands.EvalSha, sha1, args);
                }
                throw;
            }
        }

        public RedisObject EvalSHAString(string sha1, params RedisKeyValue<string, string>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            return Eval(RedisCommands.EvalSha, sha1, args);
        }

        public RedisObject EvalSHAString(ref string sha1, string script, params RedisKeyValue<string, string>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            if (String.IsNullOrWhiteSpace(script))
                return Eval(RedisCommands.EvalSha, sha1, args);

            var response = ScriptExists(sha1);
            var exists = (response != null && response.Length > 0) ? response[0] == 1L : false;

            if (!exists)
                sha1 = ScriptLoad(script);

            if (String.IsNullOrEmpty(sha1))
                return null;

            try
            {
                return Eval(RedisCommands.EvalSha, sha1, args);
            }
            catch (RedisException e)
            {
                var msg = e.Message;
                if (!String.IsNullOrEmpty(msg) &&
                    msg.StartsWith("NOSCRIPT", StringComparison.OrdinalIgnoreCase))
                {
                    sha1 = ScriptLoad(script);
                    if (!String.IsNullOrEmpty(sha1))
                        return Eval(RedisCommands.EvalSha, sha1, args);
                }
                throw;
            }
        }

        public bool ScriptDebugNo()
        {
            return ExpectOK(RedisCommands.Script, RedisCommands.Debug, RedisCommands.No);
        }

        public bool ScriptDebugSync()
        {
            return ExpectOK(RedisCommands.Script, RedisCommands.Debug, RedisCommands.Sync);
        }

        public bool ScriptDebugYes()
        {
            return ExpectOK(RedisCommands.Script, RedisCommands.Debug, RedisCommands.Yes);
        }

        public long[] ScriptExists(string sha1, params string[] sha1s)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            RedisObject response = null;
            if (sha1s.Length == 0)
                response = ExpectArray(RedisCommands.Script, RedisCommands.Exists, sha1.ToBytes());
            else
            {
                var parameters = RedisCommands.Exists
                                              .Join(sha1.ToBytes())
                                              .Join(sha1s);

                response = ExpectArray(RedisCommands.Script, RedisCommands.Exists, sha1.ToBytes());
            }

            var resultLength = sha1.Length + 1;
            var result = new long[resultLength];

            if (response != null && response.Type == RedisObjectType.Array)
            {
                var items = response.Items;
                if (items != null)
                {
                    var responseLength = response.Count;

                    for (var i = 0; i < resultLength && i < responseLength; i++)
                    {
                        var item = items[i];
                        if (item != null &&
                            item.Type == RedisObjectType.Integer)
                        {
                            var data = item.Data;
                            if (data is long)
                                result[i] = (long)data;
                            else if (data is double)
                                result[i] = (long)(double)data;
                        }
                    }
                }
            }
            return result;
        }

        public bool ScriptFush()
        {
            return ExpectOK(RedisCommands.Script, RedisCommands.Flush);
        }

        public bool ScriptKill()
        {
            return ExpectOK(RedisCommands.Script, RedisCommands.Kill);
        }

        public string ScriptLoad(string script)
        {
            if (String.IsNullOrEmpty(script))
                throw new ArgumentNullException("script");

            return ExpectBulkString(RedisCommands.Script, RedisCommands.Load, script.ToBytes());
        }

        #endregion Methods
    }
}
