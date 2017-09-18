namespace Sweet.Redis
{
    public class RedisError : RedisResult<string>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Error; } }

        #endregion Properties
    }
}
