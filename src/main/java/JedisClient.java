import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by illya on 22.10.17.
 */
public class JedisClient
{
    private static Jedis jedis;
    private static Pipeline pipe;

    static
    {
        jedis = new Jedis("141.28.68.212", 6379);
        pipe = jedis.pipelined();
    }

    private JedisClient()
    {
    }

    public static Jedis getIntance()
    {
        return jedis;
    }

    public static void writeWordToDB(Word word)
    {
        pipe.zadd("z:" + word.getWord(), 0, Objects.toString(word.getTimestamp()));
        pipe.hincrBy("h:" + word.getWord(), Objects.toString(word.getTimestamp()), word.getFrequency());
    }

    public static void writeDataToRedis()
    {
        try
        {
            for (String line : Files.readAllLines(Paths.get("./words.txt")))
            {
                JedisClient.writeWordToDB(new Word(line));

            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        pipe.multi();
        pipe.sync();
        pipe.exec();

        try
        {
            pipe.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static Map<Date, Integer> query(String word, Date from, Date to)
    {

        HashMap<Date, Integer> queriedData = new HashMap<Date, Integer>();
        String[] timestamps = jedis.zrangeByLex
                ("z:" + word,
                        "[" + Objects.toString(from.getTime()),
                        "[" + Objects.toString(to.getTime())
                ).toArray(new String[0]);
        String[] frequencies = jedis.hmget("h:" + word, timestamps).toArray(new String[0]);

        for (int i = 0; i < timestamps.length; i++)
        {
            queriedData.put(
                    new Date(Long.parseLong(timestamps[i])),
                    Integer.parseInt(frequencies[i])
            );
        }

        return queriedData;
    }
}
