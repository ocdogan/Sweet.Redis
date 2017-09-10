namespace Sweet.Redis
{
    public interface IRedisScriptingCommands
    {
        RedisObject Eval(string script, params RedisKeyValue<string, byte[]>[] args);
        RedisObject EvalString(string script, params RedisKeyValue<string, string>[] args);

        RedisObject EvalSHA(string sha1, params RedisKeyValue<string, byte[]>[] args);
        RedisObject EvalSHA(ref string sha1, string script, params RedisKeyValue<string, byte[]>[] args);
        RedisObject EvalSHAString(string sha1, params RedisKeyValue<string, string>[] args);
        RedisObject EvalSHAString(ref string sha1, string script, params RedisKeyValue<string, string>[] args);

        bool ScriptDebugYes();
        bool ScriptDebugSync();
        bool ScriptDebugNo();
        long[] ScriptExists(string sha1, params string[] sha1s);
        bool ScriptFush();
        bool ScriptKill();
        string ScriptLoad(string script);
    }
}
