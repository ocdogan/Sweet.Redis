namespace Sweet.Redis
{
    public class RedisString : RedisResult<string>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.String; } }

        #endregion Properties
    }
}
