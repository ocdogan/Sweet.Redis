namespace Sweet.Redis
{
    /*
    SADD key member [member ...]
    summary: Add one or more members to a set
    since: 1.0.0

    SCARD key
    summary: Get the number of members in a set
    since: 1.0.0

    SDIFF key [key ...]
    summary: Subtract multiple sets
    since: 1.0.0

    SDIFFSTORE destination key [key ...]
    summary: Subtract multiple sets and store the resulting set in a key
    since: 1.0.0

    SINTER key [key ...]
    summary: Intersect multiple sets
    since: 1.0.0

    SINTERSTORE destination key [key ...]
    summary: Intersect multiple sets and store the resulting set in a key
    since: 1.0.0

    SISMEMBER key member
    summary: Determine if a given value is a member of a set
    since: 1.0.0

    SMEMBERS key
    summary: Get all the members in a set
    since: 1.0.0

    SMOVE source destination member
    summary: Move a member from one set to another
    since: 1.0.0

    SPOP key [count]
    summary: Remove and return one or multiple random members from a set
    since: 1.0.0

    SRANDMEMBER key [count]
    summary: Get one or multiple random members from a set
    since: 1.0.0

    SREM key member [member ...]
    summary: Remove one or more members from a set
    since: 1.0.0

    SSCAN key cursor [MATCH pattern] [COUNT count]
    summary: Incrementally iterate Set elements
    since: 2.8.0

    SUNION key [key ...]
    summary: Add multiple sets
    since: 1.0.0

    SUNIONSTORE destination key [key ...]
    summary: Add multiple sets and store the resulting set in a key
    since: 1.0.0
     */
    public interface IRedisSetsCommands
    {
        long SAdd(string key, byte[] member, params byte[][] members);
        long SAdd(string key, string member, params string[] members);

        long SCard(string key);

        byte[][] SDiff(string fromKey, params string[] keys);
        long SDiffStore(string toKey, string fromKey, params string[] keys);
        string[] SDiffString(string fromKey, params string[] keys);

        byte[][] SInter(string key, params string[] keys);
        long SInterStore(string toKey, params string[] keys);
        string[] SInterStrings(string key, params string[] keys);

        bool SIsMember(string key, byte[] member);
        bool SIsMember(string key, string member);

        byte[][] SMembers(string key);
        string[] SMemberStrings(string key);

        bool SMove(string fromKey, string toKey, byte[] member);
        bool SMove(string fromKey, string toKey, string member);

        byte[] SPop(string key);
        string SPopString(string key);

        byte[] SRandMember(string key);
        byte[][] SRandMember(string key, int count);
        string SRandMemberString(string key);
        string[] SRandMemberString(string key, int count);

        long SRem(string key, byte[] member, params byte[][] members);
        long SRem(string key, string member, params string[] members);

        byte[][] SScan(string key, int count = 10, string match = null);
        string[] SScanString(string key, int count = 10, string match = null);

        byte[][] SUnion(string key, params string[] keys);
        long SUnionStore(string intoKey, params string[] keys);
        string[] SUnionStrings(string key, params string[] keys);
    }
}
