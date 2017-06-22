/**
 * @FileName: WorldCountRun.java
 * @Package: hadoop.mapreduce
 * @author caoshoujun
 * @created 2017/6/14 0:08
 * <p/>
 * Copyright 2016 ziroom
 */
package hadoop.mapreduce.world;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * <p></p>
 *
 * <PRE>
 * <BR>	修改记录
 * <BR>-----------------------------------------------
 * <BR>	修改日期			修改人			修改内容
 * </PRE>
 *
 * @author caoshoujun
 * @version 1.0
 * @since 1.0
 */
public class WorldCountRun {

    public static void main(String[] args) {
        final String input_path = "hdfs://node1:9000/usr/hadoop/data/input/worldCount.txt";
        final String out_path = "hdfs://node1:9000/usr/hadoop/data/output/worldCountResult";
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name","hdfs://node1:9000");
        configuration.set("mapred.job.tracker","192.168.126.128:9001");
        configuration.set("mapred.jar","E:\\hadoop-learn-java.jar");
        try {
            final FileSystem fileSystem = FileSystem.get(new URI(input_path),configuration);
            if(fileSystem.exists(new Path(out_path))){
                fileSystem.delete(new Path(out_path), true);
            }
            Job job = new Job(configuration);
            job.setJarByClass(WorldCountRun.class);
            job.setMapperClass(WorldCountMapper.class);
            job.setReducerClass(WorldCountReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(input_path));
            FileOutputFormat.setOutputPath(job, new Path(out_path));
            System.exit(job.waitForCompletion(true)?0:1);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
