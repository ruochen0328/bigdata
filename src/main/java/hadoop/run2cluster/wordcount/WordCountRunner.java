package hadoop.run2cluster.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yangwensheng
 * @date 2018/7/10 18:12
 * 参考https://blog.csdn.net/qq_28039433/article/details/78150649
 */

public class WordCountRunner {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        //集群的方式运行，非本地运行
        config.set("mapreduce.framework.name", "yarn");
        //意思是跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，去网上搜答案很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，但是其实添加了这行代码就可以了。
        config.set("mapreduce.app-submission.cross-platform", "true");
        config.set("mapreduce.job.jar","E:\\Workspace\\intellij idea\\bigdatalearning\\target\\bigdatalearning-1.0-SNAPSHOT.jar");

        Job job = Job.getInstance(config);
        job.setJarByClass(WordCountRunner.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //要处理的数据输入与输出地址
        FileInputFormat.setInputPaths(job,"hdfs://mini01:9000/zookeeper.out");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        FileOutputFormat.setOutputPath(job,new Path("hdfs://mini01:9000/output/"+ simpleDateFormat.format(new Date(System.currentTimeMillis()))));
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}