namespace Sweet.Redis
{
    /*
    EVAL script numkeys key [key ...] arg [arg ...]
    summary: Execute a Lua script server side
    since: 2.6.0

    EVALSHA sha1 numkeys key [key ...] arg [arg ...]
    summary: Execute a Lua script server side
    since: 2.6.0

    SCRIPT DEBUG YES|SYNC|NO
    summary: Set the debug mode for executed scripts.
    since: 3.2.0

    SCRIPT EXISTS script [script ...]
    summary: Check existence of scripts in the script cache.
    since: 2.6.0

    SCRIPT FLUSH -
    summary: Remove all the scripts from the script cache.
    since: 2.6.0

    SCRIPT KILL -
    summary: Kill the script currently in execution.
    since: 2.6.0

    SCRIPT LOAD script
    summary: Load the specified Lua script into the script cache.
    since: 2.6.0
     */
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
