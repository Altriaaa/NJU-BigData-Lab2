package InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexDriver
{
    public static int run(String[] args, String stopwordsPath) throws Exception
    {
        Configuration conf = new Configuration();
        if(stopwordsPath != null && !stopwordsPath.isEmpty())
        {
            conf.set("stopwordsPath", stopwordsPath);
            System.out.println("Stopwords filter enabled. Path of stopwords file: " + stopwordsPath);
        }

        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndexDriver.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
