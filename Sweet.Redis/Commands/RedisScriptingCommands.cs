using System;
using System.Text;

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

        public RedisObject Eval(string script, int numkeys, params RedisKeyValue<string, string>[] args)
        {
            throw new NotImplementedException();
        }

        public RedisObject Eval(string script, int numkeys, params RedisKeyValue<string, byte[]>[] args)
        {
            throw new NotImplementedException();
        }

        public RedisObject EvalSHA(string sha1, int numkeys, params RedisKeyValue<string, string>[] args)
        {
            throw new NotImplementedException();
        }

        public RedisObject EvalSHA(string sha1, int numkeys, params RedisKeyValue<string, byte[]>[] args)
        {
            throw new NotImplementedException();
        }

        public bool ScriptDebugNo()
        {
            throw new NotImplementedException();
        }

        public bool ScriptDebugSync()
        {
            throw new NotImplementedException();
        }

        public bool ScriptDebugYes()
        {
            throw new NotImplementedException();
        }

        public long[] ScriptExists(string sha1, params string[] sha1s)
        {
            throw new NotImplementedException();
        }

        public string ScriptFush()
        {
            throw new NotImplementedException();
        }

        public string ScriptKill()
        {
            throw new NotImplementedException();
        }

        public string ScriptLoad(string script)
        {
            throw new NotImplementedException();
        }

        #endregion Methods
    }
}
