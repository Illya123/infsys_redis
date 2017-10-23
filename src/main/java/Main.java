import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by illya on 09.10.17.
 */
@SuppressWarnings("ALL")
public class Main
{

    public static void main(String[] args)
    {
        //      Exercise3
        //      Jedis jedis = new Jedis("141.28.68.212", 6379);
        //      jedis.set("I", "Italia");
        //      jedis.close();

        // Exercise4

        // Write needs to be executed only once - format: key=word, score=0, value=timestamp
        JedisClient.getIntance().flushAll();
         JedisClient.writeDataToRedis();

        String searchWord = "hello";
        Date from = new Date(0000000000000L);
        Date to = new Date(2434827180000L);
        int wordFreq = 0;

        // Query time
        long startTime = System.nanoTime();
        Map<Date, Integer> queriedData = JedisClient.query(searchWord, from, to);
        long endTime = System.nanoTime();

        System.out.println("The query needed " + ((endTime - startTime)/1000000) + " ms");

        // Calculate the word frequency by adding all frequencies
        wordFreq = calcWordFreq(queriedData);
        printFreq(searchWord, wordFreq, from, to);
    }



    /**
     * Returns the frequency of the word
     * @param querieData
     * @return
     */
    public static int calcWordFreq(Map<Date, Integer> querieData){
        int wordSum = 0;
        for(Map.Entry<Date, Integer> entry : querieData.entrySet()){
            wordSum += entry.getValue();
        }

        return wordSum;
    }

    public static void printFreq(String searchedWord, int freq, Date from, Date to){
        DateFormat gerFormat = new SimpleDateFormat( "dd.MM.yy hh:mm:ss");

        System.out.println("In the Inverval: " + gerFormat.format(from) + "-" + gerFormat.format(to) + ", " +
                 "\""+searchedWord +"\"" + " appeared " + freq + " times");
    }
}
