﻿#region License
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

        public RedisScriptingCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        private RedisRaw Eval(byte[] cmd, RedisParam source, params RedisKeyValue<RedisParam, RedisParam>[] args)
        {
            var argsLength = args.Length;
            if (argsLength == 0)
                return ExpectArray(cmd, source, RedisConstants.ZeroBytes);

            var parameters = new byte[2 * (1 + argsLength)][];

            parameters[0] = source.Data;
            parameters[1] = argsLength.ToBytes();

            for (int i = 0, paramsIndex = 2; i < argsLength; i++, paramsIndex++)
            {
                parameters[paramsIndex] = args[i].Key.Data;
                parameters[argsLength + paramsIndex] = args[i].Value.Data;
            }

            return ExpectArray(cmd, parameters);
        }

        public RedisRaw Eval(RedisParam script, params RedisKeyValue<RedisParam, RedisParam>[] args)
        {
            if (script.IsEmpty)
                throw new ArgumentNullException("script");

            return Eval(RedisCommandList.Eval, script, args);
        }

        public RedisRaw EvalSHA(RedisParam sha1, params RedisKeyValue<RedisParam, RedisParam>[] args)
        {
            if (sha1.IsEmpty)
                throw new ArgumentNullException("sha1");

            return Eval(RedisCommandList.EvalSha, sha1, args);
        }

        public RedisRaw EvalSHA(ref RedisParam sha1, RedisParam script, params RedisKeyValue<RedisParam, RedisParam>[] args)
        {
            if (sha1.IsEmpty)
                throw new ArgumentNullException("sha1");

            if (script.IsEmpty)
                return Eval(RedisCommandList.EvalSha, sha1, args);

            var response = ScriptExists(sha1);
            var exists = (response != null && response.Length > 0) && response[0] == RedisConstants.One;

            if (!exists)
            {
                var sha1S = ScriptLoad(script);
                sha1 = new RedisParam(sha1S);
            }

            if (sha1.IsEmpty)
                return null;

            try
            {
                return Eval(RedisCommandList.EvalSha, sha1, args);
            }
            catch (RedisException e)
            {
                var msg = e.Message;
                if (!msg.IsEmpty() &&
                    msg.StartsWith("NOSCRIPT", StringComparison.OrdinalIgnoreCase))
                {
                    var sha1S = ScriptLoad(script);
                    sha1 = new RedisParam(sha1S);

                    if (!sha1.IsEmpty)
                        return Eval(RedisCommandList.EvalSha, sha1, args);
                }
                throw;
            }
        }

        public RedisBool ScriptDebugNo()
        {
            return ExpectOK(RedisCommandList.Script, RedisCommandList.Debug, RedisCommandList.No);
        }

        public RedisBool ScriptDebugSync()
        {
            return ExpectOK(RedisCommandList.Script, RedisCommandList.Debug, RedisCommandList.Sync);
        }

        public RedisBool ScriptDebugYes()
        {
            return ExpectOK(RedisCommandList.Script, RedisCommandList.Debug, RedisCommandList.Yes);
        }

        public RedisMultiInteger ScriptExists(RedisParam sha1, params RedisParam[] sha1s)
        {
            if (sha1.IsEmpty)
                throw new ArgumentNullException("sha1");

            RedisRaw response = null;
            if (sha1s.Length == 0)
                response = ExpectArray(RedisCommandList.Script, RedisCommandList.Exists, sha1);
            else
            {
                var parameters = RedisCommandList.Exists
                                              .Join(sha1)
                                              .Join(sha1s);

                response = ExpectArray(RedisCommandList.Script, RedisCommandList.Exists, sha1);
            }

            var resultLength = sha1.Length + 1;
            var result = new long[resultLength];

            if (response != null)
            {
                var responseValue = response.Value;
                if (responseValue != null && responseValue.Type == RedisRawObjectType.Array)
                {
                    var items = responseValue.Items;
                    if (items != null)
                    {
                        var responseLength = responseValue.Count;

                        for (var i = 0; i < resultLength && i < responseLength; i++)
                        {
                            var item = items[i];
                            if (item != null &&
                                item.Type == RedisRawObjectType.Integer)
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
            }
            return result;
        }

        public RedisBool ScriptFush()
        {
            return ExpectOK(RedisCommandList.Script, RedisCommandList.Flush);
        }

        public RedisBool ScriptKill()
        {
            return ExpectOK(RedisCommandList.Script, RedisCommandList.Kill);
        }

        public RedisString ScriptLoad(RedisParam script)
        {
            if (script.IsEmpty)
                throw new ArgumentNullException("script");

            return ExpectBulkString(RedisCommandList.Script, RedisCommandList.Load, script);
        }

        #endregion Methods
    }
}
