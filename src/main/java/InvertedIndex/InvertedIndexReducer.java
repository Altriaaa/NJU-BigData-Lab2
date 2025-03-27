package InvertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.text.DecimalFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
{
    private Text res = new Text();
    private static final DecimalFormat df = new DecimalFormat("0.00");

    /**
     * 对于每个单词，将其在各个文件中出现的次数进行合并
     * @param key 单词
     * @param values 单词在各个文件中出现的次数，已经过本地合并
     * @param context 结果，形如 ([key], "avg, filename:cnt; filename:cnt")
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        HashMap<String, Integer> cnt = new HashMap<>();
        int total = 0;
        // 合并统计结果
        for(Text partRes : values)
        {
            String[] entries = partRes.toString().split(";");
            for(String entry : entries)
            {
                String[] parts = entry.split(":");
                if(parts.length == 2)
                {
                    String filename = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    total += count;
                    cnt.put(filename, cnt.getOrDefault(filename, 0) + count);
                }
            }
        }
        // 生成格式化结果
        double avg = !cnt.isEmpty() ? (double)total / cnt.size() : 0;
        StringBuilder output = new StringBuilder();
        output.append(df.format(avg)).append(",").append(" ");
        for(String name : cnt.keySet())
        {
            output.append(name).append(":").append((cnt.get(name))).append(";").append(" ");
        }
        if(output.length() >= 2)
        {
            output.setLength(output.length()-2);
        }
        res.set(output.toString());
        context.write(new Text("[" + key.toString() + "]"), res);
    }
}
