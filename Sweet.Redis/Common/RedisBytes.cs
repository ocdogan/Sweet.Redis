namespace Sweet.Redis
{
    public class RedisBytes : RedisResult<byte[]>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Bytes; } }

        #endregion Properties
    }
}
