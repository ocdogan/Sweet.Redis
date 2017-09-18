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
    internal class RedisScriptingCommands : RedisCommandSet, IRedisScriptingCommands
    {
        #region .Ctors

        public RedisScriptingCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        private RedisRawObj Eval(byte[] cmd, string source, params RedisKeyValue<string, string>[] args)
        {
            var argsLength = args.Length;
            if (argsLength == 0)
                return ExpectArray(cmd, source.ToBytes(), RedisConstants.ZeroBytes);

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

        private RedisRawObj Eval(byte[] cmd, string source, params RedisKeyValue<string, byte[]>[] args)
        {
            var argsLength = args.Length;
            if (argsLength == 0)
                return ExpectArray(cmd, source.ToBytes(), RedisConstants.ZeroBytes);

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

        public RedisRawObj Eval(string script, params RedisKeyValue<string, byte[]>[] args)
        {
            if (String.IsNullOrEmpty(script))
                throw new ArgumentNullException("script");

            return Eval(RedisCommands.Eval, script, args);
        }

        public RedisRawObj EvalString(string script, params RedisKeyValue<string, string>[] args)
        {
            if (String.IsNullOrEmpty(script))
                throw new ArgumentNullException("script");

            return Eval(RedisCommands.Eval, script, args);
        }

        public RedisRawObj EvalSHA(string sha1, params RedisKeyValue<string, byte[]>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            return Eval(RedisCommands.EvalSha, sha1, args);
        }

        public RedisRawObj EvalSHA(ref string sha1, string script, params RedisKeyValue<string, byte[]>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            if (String.IsNullOrWhiteSpace(script))
                return Eval(RedisCommands.EvalSha, sha1, args);

            var response = ScriptExists(sha1);
            var exists = (response != null && response.Length > 0) ? response[0] == RedisConstants.One : false;

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

        public RedisRawObj EvalSHAString(string sha1, params RedisKeyValue<string, string>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            return Eval(RedisCommands.EvalSha, sha1, args);
        }

        public RedisRawObj EvalSHAString(ref string sha1, string script, params RedisKeyValue<string, string>[] args)
        {
            if (String.IsNullOrEmpty(sha1))
                throw new ArgumentNullException("sha1");

            if (String.IsNullOrWhiteSpace(script))
                return Eval(RedisCommands.EvalSha, sha1, args);

            var response = ScriptExists(sha1);
            var exists = (response != null && response.Length > 0) ? response[0] == RedisConstants.One : false;

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

            RedisRawObj response = null;
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

            if (response != null && response.Type == RedisRawObjType.Array)
            {
                var items = response.Items;
                if (items != null)
                {
                    var responseLength = response.Count;

                    for (var i = 0; i < resultLength && i < responseLength; i++)
                    {
                        var item = items[i];
                        if (item != null &&
                            item.Type == RedisRawObjType.Integer)
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
