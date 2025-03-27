package Sorter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SorterMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
{
    private final DoubleWritable avgKey = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        int tabIndex = line.indexOf('\t');
        if(tabIndex > 0)
        {
            String detail = line.substring(tabIndex+1);
            int comIndex = detail.indexOf(',');
            if(comIndex > 0)
            {
                try
                {
                    double avg = Double.parseDouble(detail.substring(0, comIndex));
                    avgKey.set(-avg);
                    context.write(avgKey, value);
                } catch (NumberFormatException ignored) {}
            }
        }
    }
}
