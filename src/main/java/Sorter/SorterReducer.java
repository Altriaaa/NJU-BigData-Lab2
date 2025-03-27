package Sorter;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SorterReducer extends Reducer<DoubleWritable, Text, Text, Text>
{
    private Text word = new Text();

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        for (Text value : values)
        {
            String line = value.toString();
            int tabIndex = line.indexOf('\t');
            if (tabIndex > 0)
            {
                word.set(line.substring(0, tabIndex));
                context.write(word, new Text(line.substring(tabIndex + 1)));
            }
        }
    }
}
