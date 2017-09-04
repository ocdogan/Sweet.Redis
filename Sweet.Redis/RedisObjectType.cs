namespace Sweet.Redis
{
    public enum RedisObjectType : int
    {
        Undefined,
        SimpleString,
        Error,
        Integer,
        BulkString,
        Array
    }
}
