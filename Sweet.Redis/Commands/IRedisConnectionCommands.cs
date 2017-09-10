namespace Sweet.Redis
{
    /*
    AUTH password
    summary: Authenticate to the server
    since: 1.0.0

    ECHO message
    summary: Echo the given string
    since: 1.0.0

    PING [message]
    summary: Ping the server
    since: 1.0.0

    QUIT -
    summary: Close the connection
    since: 1.0.0

    SELECT index
    summary: Change the selected database for the current connection
    since: 1.0.0
     */
    public interface IRedisConnectionCommands
    {
        bool Auth(string password);

        string Echo(string msg);
        string Ping();
        string Ping(string msg);
        bool Quit();
    }
}
