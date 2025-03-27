package InvertedIndex;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
{
    private Text outputValue = new Text();

    /**
     * 对于每个单词，将其在各个文件中出现的次数进行本地合并
     * @param key 单词
     * @param values 出现该单词的文件列表
     * @param context 结果，形如 (key, "filename:cnt;...")
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        HashMap<String, Integer> cnt = new HashMap<>();
        // 统计每个文件(value)中，当前单词(key)出现的次数
        for (Text value : values)
        {
            String filename = value.toString();
            cnt.put(filename, cnt.getOrDefault(filename, 0) + 1);
        }
        // 将统计结果拼接成字符串
        StringBuilder sb = new StringBuilder();
        for (String filename : cnt.keySet())
        {
            sb.append(filename).append(":").append(cnt.get(filename)).append(";");
        }
        if(sb.length() > 0)
            sb.deleteCharAt(sb.length() - 1);

        outputValue.set(sb.toString());
        context.write(key, outputValue);    // 形如 (key, "filename:cnt;...")
    }
}
