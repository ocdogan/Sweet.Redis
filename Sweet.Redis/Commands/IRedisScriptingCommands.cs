namespace Sweet.Redis
{
    public interface IRedisScriptingCommands
    {
        RedisObject Eval(string script, int numkeys, params RedisKeyValue<string, string>[] args);
        RedisObject Eval(string script, int numkeys, params RedisKeyValue<string, byte[]>[] args);

        RedisObject EvalSHA(string sha1, int numkeys, params RedisKeyValue<string, string>[] args);
        RedisObject EvalSHA(string sha1, int numkeys, params RedisKeyValue<string, byte[]>[] args);

        bool ScriptDebugYes();
        bool ScriptDebugSync();
        bool ScriptDebugNo();
        long[] ScriptExists(string sha1, params string[] sha1s);
        string ScriptFush();
        string ScriptKill();
        string ScriptLoad(string script);
    }
}
