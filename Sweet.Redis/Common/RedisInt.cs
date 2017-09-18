namespace Sweet.Redis
{
    public class RedisInt : RedisResult<long>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Integer; } }

        #endregion Properties
    }
}
