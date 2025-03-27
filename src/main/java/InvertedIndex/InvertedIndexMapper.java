package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private Text word = new Text();
    private Text filename = new Text();

    @Override
    protected void setup(Context context) throws IOException
    {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename.set(fileSplit.getPath().getName());
    }

    /**
     * 输入文本分割成单词，并为每个单词创建一个与当前文件名关联的键值对
     * @param key 输入行的偏移量
     * @param value 输入的文本行
     * @param context 输出形式为(word, filename)的键值对
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreElements())
        {
            word.set(itr.nextToken());
            context.write(word, filename);
        }
    }
}

