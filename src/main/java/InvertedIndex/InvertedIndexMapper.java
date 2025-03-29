package InvertedIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private Text word = new Text();
    private Text filename = new Text();
    private Set<String> stopwords = new HashSet<>();
    private String currentFileName;
    private static final String STOPWORDS_FILE_NAME = "cn_stopwords.txt";

    /**
     * 在Mapper的setup方法中读取停用词文件
     * @param context 上下文对象
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // 获取当前文件的名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        currentFileName = fileSplit.getPath().getName();
        filename.set(currentFileName);
        // 检查是否有停用词文件路径
        String stopwordsPath = context.getConfiguration().get("stopwordsPath");
        if (stopwordsPath != null && !stopwordsPath.isEmpty())
        {
            // 读取停用词文件
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(stopwordsPath);
            if (fs.exists(path))
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = reader.readLine()) != null)
                {
                    stopwords.add(line.trim());
                }
                reader.close();
                System.out.println("已加载 " + stopwords.size() + " 个停用词");
            }
            else
            {
                System.err.println("停用词文件不存在: " + stopwordsPath);
            }
        }
    }

    /**
     * 将输入文本分割成单词，并为每个单词创建一个与当前文件名关联的键值对
     * @param key 输入行的偏移量
     * @param value 输入的文本行
     * @param context 输出形式为(word, filename)的键值对
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        if(STOPWORDS_FILE_NAME.equals(currentFileName))
        {
            return; // 如果当前文件是停用词文件，则跳过，确保停用词文件不会被当作输入文件处理
        }
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreElements())
        {
            String token = itr.nextToken();
            // 如果停用词过滤器启用，并且当前单词在停用词列表中，则跳过
            if (!stopwords.isEmpty() && stopwords.contains(token))
            {
                continue;
            }
            word.set(token);
            context.write(word, filename);
        }
    }
}

