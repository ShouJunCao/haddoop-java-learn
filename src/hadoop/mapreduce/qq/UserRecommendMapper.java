/**
 * @FileName: UserRecommendMapper.java
 * @Package: hadoop.mapreduce.qq
 * @author caoshoujun
 * @created 2017/6/16 9:52
 * <p/>
 * Copyright 2016 ziroom
 */
package hadoop.mapreduce.qq;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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
public class UserRecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] array = line.split("\\s+");
        context.write(new Text(array[0]),new Text(array[1]));

        context.write(new Text(array[1]),new Text(array[0]));
    }
}
