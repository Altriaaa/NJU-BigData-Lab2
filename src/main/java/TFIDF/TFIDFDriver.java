package TFIDF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFDriver
{
    public static int run(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        int docCnt = 0;
        for (FileStatus fileStatus : status)
        {
            if (!fileStatus.getPath().getName().equals("cn_stopwords.txt"))
            {
                docCnt++;
            }
        }
        conf.setInt("docCnt", docCnt);
        System.out.println("文档总数: " + docCnt);

        Job job = Job.getInstance(conf, "TFIDF");

        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
