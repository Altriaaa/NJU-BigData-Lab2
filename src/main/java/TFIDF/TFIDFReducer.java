package TFIDF;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

/**
 * TF-IDF计算的Reducer类
 * 负责汇总单词在各文档中的出现频率，并计算TF-IDF值
 * 输入: <单词, 文档名称#词频>
 * 输出: <文档名称, 单词\tTF-IDF值>
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, Text>
{
    private Text resKey = new Text();
    private Text resVal = new Text();
    private int docCnt;
    private HashMap<String, Integer> docInfo = new HashMap<>();

    /**
     * 初始化Reducer，从配置中获取文档总数
     * @param context 上下文对象，包含配置信息
     */
    @Override
    protected void setup(Context context)
    {
        docCnt = context.getConfiguration().getInt("docCnt", 1);
    }

    /**
     * 计算单词的TF-IDF值并输出结果
     * @param key 输入的键，表示单词
     * @param values 输入的值列表，每个值表示单词在某个文档中的出现次数
     * @param context 上下文对象，用于输出结果
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        docInfo.clear();
        // 统计单词在各个文档中的出现次数
        for (Text val : values)  // val 形如 "filename#cnt"
        {
            String[] info = val.toString().split("#");
            if (info.length == 2)
            {
                int cnt = Integer.parseInt(info[1]);
                docInfo.put(info[0], docInfo.getOrDefault(info[0], 0) + cnt);
            }
        }
        int relatingDocNum = docInfo.size();
        double idf = Math.log10((double) docCnt / (relatingDocNum+1));
        // 计算每个文档中该单词的TF-IDF值并输出
        for(Map.Entry<String, Integer> entry : docInfo.entrySet())
        {
            String filename = entry.getKey();
            int tf = entry.getValue();
            double TF_IDF = tf * idf;
            resKey.set(filename);
            resVal.set(key.toString() + "\t" + String.format("%.2f", TF_IDF));
            context.write(resKey, resVal);
        }
    }
}
