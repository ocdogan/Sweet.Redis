using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

using Sweet.Redis;

namespace Sweet.Redis.ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // PerformanceTest();

            // ScriptingNoArgsEvalTest();
            // ScriptingShaNoArgsEvalTest();
            // ScriptingWithArgsEvalTest();
            // ScriptingShaWithArgsEvalTest();

            // PubSubTest1();
            // PubSubTest2();
            // PubSubTest3();
            // PubSubTest4();
            // PubSubTest5();
            // PubSubTest6();
            // PubSubTest7();
            // PubSubTest8();
            // PubSubTest9();
            // PubSubTest10();
            PubSubTest11();
            PubSubTest12();

            // MultiThreading1();
            // MultiThreading2();
        }

        #region Multi-Threading

        static void MultiThreading2()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 5, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("large_text", largeText);
                    db.Strings.Get("large_text");
                }

                const int ThreadCount = 50;

                var loopIndex = 0;
                List<Thread> thList = null;
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();

                        var oldThList = thList;
                        if (oldThList != null)
                        {
                            for (var i = 0; i < oldThList.Count; i++)
                            {
                                var th = oldThList[i];
                                try
                                {
                                    if (th.IsAlive)
                                        th.Interrupt();
                                }
                                catch (Exception)
                                { }
                            }
                        }

                        thList = new List<Thread>(ThreadCount);
                        try
                        {
                            for (var i = 0; i < ThreadCount; i++)
                            {
                                var th = new Thread((obj) =>
                                {
                                    var tupple = (Tuple<Thread, IRedisDb>)obj;

                                    var @this = tupple.Item1;
                                    var rdb = tupple.Item2;

                                    var sw = new Stopwatch();

                                    for (var j = 0; j < 100; j++)
                                    {
                                        sw.Restart();
                                        var data = rdb.Strings.Get("large_text");
                                        sw.Stop();

                                        Console.WriteLine(@this.Name + ": Get, " + sw.ElapsedMilliseconds + " msec, " + (data != null && data.Length == largeText.Length ? "OK" : "FAILED"));
                                    }
                                });

                                th.Name = loopIndex++ + "." + i;
                                th.IsBackground = true;
                                thList.Add(th);
                            }

                            for (var i = 0; i < thList.Count; i++)
                            {
                                var th = thList[i];
                                th.Start(new Tuple<Thread, IRedisDb>(th, db));
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void MultiThreading1()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 5, idleTimeout: 5)))
            {
                const int ThreadCount = 30;

                var loopIndex = 0;
                List<Thread> thList = null;
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();

                        var oldThList = thList;
                        if (oldThList != null)
                        {
                            for (var i = 0; i < oldThList.Count; i++)
                            {
                                var th = oldThList[i];
                                try
                                {
                                    if (th.IsAlive)
                                        th.Interrupt();
                                }
                                catch (Exception)
                                { }
                            }
                        }

                        thList = new List<Thread>(ThreadCount);
                        try
                        {
                            for (var i = 0; i < ThreadCount; i++)
                            {
                                var th = new Thread((obj) =>
                                {
                                    var tupple = (Tuple<Thread, IRedisDb>)obj;

                                    var @this = tupple.Item1;
                                    var rdb = tupple.Item2;

                                    var sw = new Stopwatch();

                                    for (var j = 0; j < 1000; j++)
                                    {
                                        sw.Restart();
                                        var result = rdb.Connection.Ping();
                                        sw.Stop();

                                        Console.WriteLine(@this.Name + ": Ping, " + sw.ElapsedMilliseconds + " msec, " + result);
                                    }
                                });

                                th.Name = loopIndex++ + "." + i;
                                th.IsBackground = true;
                                thList.Add(th);
                            }

                            for (var i = 0; i < thList.Count; i++)
                            {
                                var th = thList[i];
                                th.Start(new Tuple<Thread, IRedisDb>(th, db));
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }


        #endregion Multi-Threading

        #region PubSub Tests

        static void PubSubTest12()
        {
            var largeText = Encoding.UTF8.GetBytes(new string('x', 100000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 1000; i++)
                        db.PubSubs.Publish("abc", largeText);
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest11()
        {
            var smallText = Encoding.UTF8.GetBytes(new string('x', 1000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 1000; i++)
                        db.PubSubs.Publish("abc", smallText);
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest10()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe();

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest9()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe("ab*");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe("xy*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest8()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*");

                Thread.Sleep(5000);

                pool.PubSubChannel.PUnsubscribe("ab*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest7()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xy*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest6()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest5()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Thread.Sleep(4000);

                pool.PubSubChannel.Unsubscribe();

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest4()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Thread.Sleep(4000);

                pool.PubSubChannel.Unsubscribe("xyz");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            Console.WriteLine("Channels:");

                            var result = db.PubSubs.PubSubChannels();
                            if (result == null || result.Length == 0)
                                Console.WriteLine("(nil)");
                            else
                            {
                                for (var i = 0; i < result.Length; i++)
                                {
                                    var channel = result[i];
                                    Console.WriteLine((i + 1) + ") " + (String.IsNullOrEmpty(channel) ? "(nil)" : channel));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        try
                        {
                            Console.WriteLine("Numer of Subscribers:");

                            var result = db.PubSubs.PubSubNumerOfSubscribers("xyz");
                            if (result == null || result.Length == 0)
                                Console.WriteLine("(nil)");
                            else
                            {
                                for (var i = 0; i < result.Length; i++)
                                {
                                    var channel = result[i];
                                    Console.WriteLine((i + 1) + ") " + channel.Key + ": " + channel.Value);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        try
                        {
                            Console.Write("Numer of Pattern Subscribers: ");
                            Console.WriteLine(db.PubSubs.PubSubNumerOfSubscriptionsToPatterns());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion PubSub Tests

        #region Scripting Tests

        static void ScriptingNoArgsEvalTest()
        {
            var script = "return {1,2,3.3333,'foo',nil,'bar'}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.Eval(script);
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void ScriptingWithArgsEvalTest()
        {
            var script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalString(script,
                                                                 new RedisKeyValue<string, string>("key1", "first"),
                                                                 new RedisKeyValue<string, string>("key2", "second"));
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void ScriptingShaNoArgsEvalTest()
        {
            var script = "return {1,2,3.3333,'foo',nil,'bar'}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHA(sha1);
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void ScriptingShaWithArgsEvalTest()
        {
            var script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHAString(sha1,
                                                                 new RedisKeyValue<string, string>("key1", "first"),
                                                                 new RedisKeyValue<string, string>("key2", "second"));
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Scripting Tests

        #region Performance Tests

        static void PerformanceTest()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("large_text", largeText);
                    db.Strings.Get("large_text");
                }

                do
                {
                    Console.Clear();
                    try
                    {
                        /* using (var db = pool.GetDb())
                        {
                            WriteResult("Ping, ", db.Connection.Ping());
                            WriteResult("Ping 1, ", db.Connection.Ping("1"));
                            WriteResult("Ping String.Empty, ", db.Connection.Ping(String.Empty));
                            WriteResult("Echo 1, ", db.Connection.Echo("1"));
                            WriteResult("Echo String.Empty, ", db.Connection.Echo(String.Empty));
                            WriteResult("LastSave, ", db.Server.LastSave());
                            WriteResult("Time, ", db.Server.Time());

                            var cfg = db.Server.ConfigGet("*");
                            if (cfg == null || cfg.Count == 0)
                                WriteResult("ConfigGet *, ", "(null)");
                            else
                            {
                                WriteResult("ConfigGet *, ", "*");

                                var i = 1;
                                foreach (var kv in cfg)
                                {
                                    WriteResult("ConfigGet " + i++ + ") ",
                                                kv.Key + " = " + kv.Value);
                                }
                            }

                            var clients = db.Server.ClientListDictionary();
                            if (clients == null || clients.Length == 0)
                                WriteResult("ClientList, ", "(null)");
                            else
                            {
                                var i = 0;
                                foreach (var client in clients)
                                {
                                    i++;

                                    var j = 1;
                                    foreach (var kv in client)
                                    {
                                        WriteResult("ClientList " + i + ", " + j++ + ") ",
                                                    kv.Key + " = " + kv.Value);
                                    }
                                }
                            }
                        } */

                        /* using (var db = pool.GetDb(1))
                        {
                            WriteResult("Set key1 Hello, ", db.Strings.Set("key1", "Hello"));
                            WriteResult("Set key2 World, ", db.Strings.Set("key2", "World"));
                            WriteResult("Set key3 Hello\\r\\nWorld, ", db.Strings.Set("key3", "Hello\r\nWorld"));
                            WriteResult("Set key4 largeText, ", db.Strings.Set("key4", largeText));

                            WriteResult("GetString key1, ", db.Strings.GetString("key1"));
                            WriteResult("GetString key2, ", db.Strings.GetString("key2"));
                            WriteResult("GetString key3, ", db.Strings.GetString("key3"));
                            // WriteResult("GetString key4 largeText, ", db.Strings.GetString("key4"));

                            ConsoleWriteMultiline("MGet key1, ", db.Strings.MGet("key1"));
                            ConsoleWriteMultiline("MGet key1 key2, ", db.Strings.MGet("key1", "key2"));
                            ConsoleWriteMultiline("MGet key1 key2 key3, ", db.Strings.MGet("key1", "key2", "key3"));
                            // ConsoleWriteMultiline("MGet key1 key2 key3 key4, ", db.Strings.MGet("key1", "key2", "key3", "key4"));
                        } */

                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Connection.Ping();
                            // db.Strings.Get("large_text");

                            /* for (var i = 0; i < 1000; i++)
                                db.Connection.Ping(); */

                            /* {
                                Console.WriteLine((i + 1).ToString() + ") ");
                                try
                                {
                                    Console.WriteLine((i + 1).ToString() + ") " + db.Connection.Ping());
                                    // Console.WriteLine(db.Strings.GetString("key4"));
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e);
                                }
                                Console.WriteLine();
                            } */
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds + " msec");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        #endregion Performance Tests

        #region Generic

        private static void WriteResult(string cmd, object result)
        {
            var str = (result == null) ? String.Empty : (result.ToString() ?? String.Empty);

            Console.Write(cmd);
            Console.WriteLine(str);
            Console.WriteLine("Length: " + str.Length);
            Console.WriteLine("-----------------------------------------------");
            Console.WriteLine();
        }

        private static void ConsoleWriteMultiline(string cmd, string[] strArray)
        {
            Console.Write(cmd);
            if (strArray == null)
                Console.WriteLine("1..) (null)");
            else
            {
                for (var i = 0; i < strArray.Length; i++)
                {
                    var str = strArray[i];
                    if (str == null)
                        Console.Write((i + 1) + ") (null)");
                    else
                        Console.Write((i + 1) + ") " + str);

                    if (i < strArray.Length - 1)
                        Console.Write(", ");

                    Console.WriteLine();
                }
            }
            Console.WriteLine("-----------------------------------------------");
            Console.WriteLine();
        }

        #endregion Generic
    }
}
