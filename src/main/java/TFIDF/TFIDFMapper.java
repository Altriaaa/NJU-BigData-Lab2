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

    /**
     * 初始化Mapper，获取当前文件名
     * @param context 上下文对象，包含配置信息
     */
    @Override
    protected void setup(Context context)
    {
        filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    }

    /**
     * 处理每一行数据，统计单词出现次数
     * @param key 输入的键，表示行号
     * @param value 输入的值，表示行内容
     */
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

    /**
     * 在Mapper结束时输出每个单词及其在当前文件中的出现次数
     * @param context 上下文对象，用于输出结果
     */
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
