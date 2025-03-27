package TFIDF;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text>
{
    private Text resKey = new Text();
    private Text resVal = new Text();
    private int docCnt;
    private HashMap<String, Integer> docInfo = new HashMap<>();

    @Override
    protected void setup(Context context)
    {
        docCnt = context.getConfiguration().getInt("docCnt", 1);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        docInfo.clear();
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
