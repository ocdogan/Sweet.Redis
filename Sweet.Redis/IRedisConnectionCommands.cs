namespace Sweet.Redis
{
    public interface IRedisConnectionCommands
    {
        bool Auth(string password);

        string Echo(string msg);
        string Ping();
        string Ping(string msg);
        bool Quit();
    }
}
