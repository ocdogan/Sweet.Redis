namespace Sweet.Redis
{
    public class RedisRaw : RedisResult<RedisRawObj>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Raw; } }

        #endregion Properties
    }
}
