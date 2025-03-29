package TFIDF;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private Text wordKey = new Text();
    private Text docInfo = new Text();
    private String filename;
    private HashMap<String, Integer> wordCnt = new HashMap<>();
    private static final String STOPWORDS_FILE_NAME = "cn_stopwords.txt";

    @Override
    protected void setup(Context context)
    {
        filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 如果当前文件是停用词文件，则跳过处理
        if(STOPWORDS_FILE_NAME.equals(filename))
        {
            return;
        }
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens())
        {
            String word = itr.nextToken();
            wordCnt.put(word, wordCnt.getOrDefault(word, 0)+1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        // 如果当前文件是停用词文件，则不输出任何内容
        if(STOPWORDS_FILE_NAME.equals(filename))
        {
            return;
        }
        for(Map.Entry<String, Integer> entry : wordCnt.entrySet())
        {
            wordKey.set(entry.getKey());
            docInfo.set(filename + "#" + entry.getValue());
            context.write(wordKey, docInfo);
        }
    }
}
