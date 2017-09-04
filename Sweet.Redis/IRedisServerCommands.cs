using System;
using System.Collections.Generic;

namespace Sweet.Redis
{
    public interface IRedisServerCommands
    {
		bool BGRewriteAOF();
		bool BGSave();

		string ClientGetName();
        long ClientKill(string ip = null, int port = -1, string clientId = null, string type = null, bool skipMe = true);
        RedisClientInfo[] ClientList();
        IDictionary<string, string>[] ClientListDictionary();
        bool ClientPause(int timeout);
        bool ClientReplyOn();
        bool ClientReplyOff();
        bool ClientReplySkip();
        bool ClientSetName(string connectionName);

        IDictionary<string, string> ConfigGet(string parameter);
        bool ConfigResetStat();
        bool ConfigRewrite();
        bool ConfigSet(string parameter, string value);

		long DbSize();
		
        bool FlushAll();
        bool FlushAllAsync();
		bool FlushDb();
        bool FlushDbAsync();

		string[] Info(string section);
        DateTime LastSave();
		bool Save();
		void ShutDown();
        void ShutDownSave();
		bool SlaveOf(string host, int port);
        bool SlaveOfNoOne();
		DateTime Time();
	}
}
