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
            // PerformanceTest1();
            // PerformanceTest2();

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
            // PubSubTest11();
            // PubSubTest12();

            // MultiThreading1();
            // MultiThreading2a();
            // MultiThreading2b();
            // MultiThreading3();
            // MultiThreading4();
            // MultiThreading5();
            // MultiThreading6();
            // MultiThreading7();

            // MonitorTest1();
            // MonitorTest2();
            // MonitorTest3();

            // Geo1();

            // SlowLog1();

            // Info1();

            // Shutdown();

            Transaction1();
        }

        #region Transaction

        static void Transaction1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var transaction = pool.BeginTransaction())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var list = new List<RedisResult>();
                            for (var i = 0; i < 100; i++)
                            {
                                var result = transaction.Connection.Ping(i + 1);
                                list.Add(result);
                            }

                            transaction.Execute();

                            for (var i = 0; i < list.Count; i++)
                                Console.WriteLine(list[i]);
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

        #endregion Transaction

        #region Shutdown

        static void Shutdown()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();
                            db.Server.ShutDown();
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

        #endregion Shutdown

        #region Info

        static void Info1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var infoResult = db.Server.Info();

                            if (infoResult == null || infoResult.Value == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                var info = infoResult.Value;

                                foreach (var kv1 in info)
                                {
                                    Console.WriteLine("# " + kv1.Key);

                                    var section = kv1.Value;
                                    if (section != null)
                                    {
                                        foreach (var kv2 in section)
                                            Console.WriteLine(kv2.Key + ":" + kv2.Value);
                                    }

                                    Console.WriteLine();
                                }
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

        #endregion Info

        #region SlowLog

        static void SlowLog1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var lenResult = db.Server.SlowLogLen();

                            Console.WriteLine("SLOWLOG LEN");
                            Console.WriteLine(lenResult);

                            if (lenResult > 0)
                            {
                                var getResult = db.Server.SlowLogGet(lenResult);

                                Console.WriteLine("SLOWLOG GET " + lenResult);
                                Console.WriteLine(getResult);
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

        #endregion SlowLog

        #region Geo

        static void Geo1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var addResult = db.Geo.GeoAdd("Sicily",
                                          new RedisGeospatialItem(13.361389, 38.115556, "Palermo"),
                                          new RedisGeospatialItem(15.087269, 37.502669, "Catania"));

                            Console.WriteLine("GEOADD Sicily 13.361389 38.115556 \"Palermo\" 15.087269 37.502669 \"Catania\"");
                            Console.WriteLine(addResult);

                            var distResult = db.Geo.GeoDistance("Sicily", "Palermo", "Catania");
                            Console.WriteLine("GEODIST Sicily Palermo Catania");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistance("Sicily", "Palermo", "Catania", RedisGeoDistanceUnit.Kilometers);
                            Console.WriteLine("GEODIST Sicily Palermo Catania km");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistance("Sicily", "Palermo", "Catania", RedisGeoDistanceUnit.Miles);
                            Console.WriteLine("GEODIST Sicily Palermo Catania mi");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistance("Sicily", "Foo", "Bar");
                            Console.WriteLine("GEODIST Sicily Foo Bar");
                            Console.WriteLine(distResult);

                            var hashResult = db.Geo.GeoHash("Sicily", "Palermo", "Catania");
                            Console.WriteLine("GEOHASH Sicily Palermo Catania");
                            Console.WriteLine(hashResult);

                            var posResult = db.Geo.GeoPosition("Sicily", "Palermo", "Catania", "NonExisting");
                            Console.WriteLine("GEOPOS Sicily Palermo Catania NonExisting");
                            Console.WriteLine(posResult);

                            var radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers, withDist: true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHDIST");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers, withCoord: true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                        200, RedisGeoDistanceUnit.Kilometers, true, true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                            200, RedisGeoDistanceUnit.Kilometers, true, true, true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST WITHHASH");
                            Console.WriteLine(radiusResult);
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

        #endregion Geo

        #region Multi-Threading

        static void MultiThreading7()
        {
            MultiThreadingBase(12, 2, 1, 5000, "medium_text", new string('x', 5000), true, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("medium_text"); });
        }

        static void MultiThreading6()
        {
            MultiThreadingBase(12, 1, 50, 100, "tiny_text", new string('x', 20), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("tiny_text"); });
        }

        static void MultiThreading5()
        {
            MultiThreadingBase(12, 1, 50, 100, "small_text", new string('x', 1000), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("small_text"); });
        }

        static void MultiThreading4()
        {
            MultiThreadingBase(12, 1, 50, 100, "small_text", new string('x', 1000), true, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("small_text"); });
        }

        static void MultiThreading3()
        {
            MultiThreadingBase(12, 10, 50, 100, "small_text", new string('x', 1000), true, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("small_text"); });
        }

        static void MultiThreading2a()
        {
            MultiThreadingBase(12, 10, 50, 100, "large_text", new string('x', 100000), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("large_text"); });
        }

        static void MultiThreading2b()
        {
            MultiThreadingBase(12, 1, 50, 100, "large_text", new string('x', 100000), false, 5,
                               (rdb, dbIndex) => { return rdb.Strings.Get("large_text"); });
        }

        static void MultiThreading1()
        {
            var tinyText = new string('x', 10);

            MultiThreadingBase(12, 1, 50, 100, "tiny_text", tinyText, true, 5,
                               (rdb, dbIndex) => { return Encoding.UTF8.GetBytes(rdb.Connection.Ping(tinyText).Value); });
        }

        static void MultiThreadingBase(int dbIndex, int maxCount, int threadCount, int loopCount,
                                       string testKey, string testText, bool requireKeyPress,
                                       int sleepSecs, Func<IRedisDb, int, byte[]> proc)
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                     // new RedisSettings(host: "172.28.10.233", port: 6381, maxCount: maxCount))) // DEV
                     new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: maxCount, useAsyncCompleter: false))) // LOCAL
            {
                using (var db = pool.GetDb(dbIndex))
                {
                    var b = db.Strings.Set(testKey, testText);
                    if (!b)
                        throw new Exception("can not set");

                    var g = db.Strings.Get(testKey);
                    if (g == null || g.Value == null || g.Value.Length != (testText ?? "").Length)
                        throw new Exception("can not get");
                }

                var loopIndex = 0;
                List<Thread> thList = null;

                var totalSw = new Stopwatch();
                using (var db = pool.GetDb(dbIndex))
                {
                    do
                    {
                        totalSw.Reset();

                        var ticks = 0L;
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

                        thList = new List<Thread>(threadCount);
                        try
                        {
                            loopIndex = 0;
                            var ticksLock = new object();

                            for (var i = 0; i < threadCount; i++)
                            {
                                var th = new Thread((obj) =>
                                {
                                    var tupple = (Tuple<Thread, IRedisDb, AutoResetEvent>)obj;

                                    var autoReset = tupple.Item3;
                                    try
                                    {
                                        var @this = tupple.Item1;
                                        var rdb = tupple.Item2;

                                        var sw = new Stopwatch();

                                        for (var j = 0; j < loopCount; j++)
                                        {
                                            sw.Restart();
                                            var data = proc(rdb, dbIndex);
                                            sw.Stop();

                                            Console.WriteLine(@this.Name + ": Processed, " + sw.ElapsedMilliseconds.ToString("D3") + " msec, " +
                                                (data != null && data.Length == (testText ?? "").Length ? "OK" : "FAILED"));

                                            lock (ticksLock)
                                            {
                                                ticks += sw.ElapsedTicks;
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        autoReset.Set();
                                    }
                                });

                                th.Name = loopIndex++.ToString("D2") + "." + i.ToString("D2");
                                th.IsBackground = true;
                                thList.Add(th);
                            }

                            totalSw.Restart();

                            var autoResets = new AutoResetEvent[thList.Count];
                            try
                            {
                                for (var i = 0; i < thList.Count; i++)
                                {
                                    var th = thList[i];

                                    var autoReset = new AutoResetEvent(false);
                                    autoResets[i] = autoReset;

                                    th.Start(new Tuple<Thread, IRedisDb, AutoResetEvent>(th, db, autoReset));
                                }
                            }
                            finally
                            {
                                WaitHandle.WaitAll(autoResets);
                                totalSw.Stop();
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();

                        Console.WriteLine("Sum of inner ticks: " + ticks);
                        Console.WriteLine("Total ticks:        " + totalSw.ElapsedTicks);

                        if (requireKeyPress)
                            Console.WriteLine("Press any key to continue, ESC to escape ...");
                        else
                            Thread.Sleep(sleepSecs * 1000);
                    }
                    while (!requireKeyPress || Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Multi-Threading

        #region Monitor Tests

        static void MonitorTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("1");
                }

                Thread.Sleep(1000);

                pool.MonitorChannel.Quit();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("2");
                }

                Thread.Sleep(1000);
                pool.MonitorChannel.Monitor();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("3");
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void MonitorTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping();
                }

                Thread.Sleep(5000);

                pool.MonitorChannel.Quit();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping();
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void MonitorTest1()
        {
            var largeText = Encoding.UTF8.GetBytes(new string('x', 100000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        #endregion Monitor Tests

        #region PubSub Tests

        static void PubSubTest12()
        {
            var largeText = Encoding.UTF8.GetBytes(new string('x', 100000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                            if (result == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                var value = result.Value;
                                if (value == null || value.Length == 0)
                                    Console.WriteLine("(nil)");
                                else
                                {
                                    for (var i = 0; i < value.Length; i++)
                                    {
                                        var channel = value[i];
                                        Console.WriteLine((i + 1) + ") " + channel.Key + ": " + channel.Value);
                                    }
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.Eval(script,
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key1", "first"),
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key2", "second"));
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHA(sha1.Value);
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
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHA(sha1.Value,
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key1", "first"),
                                                                 new RedisKeyValue<RedisParam, RedisParam>("key2", "second"));
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

        static void PerformanceTest2()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
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
                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Strings.Get("large_text");
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds.ToString("D3") + " msec");
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

        static void PerformanceTest1()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    db.Connection.Ping();
                }

                do
                {
                    Console.Clear();
                    try
                    {
                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Connection.Ping();
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds.ToString("D3") + " msec");
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
