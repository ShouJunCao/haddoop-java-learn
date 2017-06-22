/**
 * @FileName: UserRecommendReducer.java
 * @Package: hadoop.mapreduce.qq
 * @author caoshoujun
 * @created 2017/6/16 11:34
 * <p/>
 * Copyright 2016 ziroom
 */
package hadoop.mapreduce.qq;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
public class UserRecommendReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<>();

        for (Text text : values){
            set.add(text.toString());
        }
        if(set.size()>1){
            for (Iterator<String> it = set.iterator() ; it.hasNext();){
                String qqName = it.next();
                for (Iterator<String> j = set.iterator();j.hasNext();){
                    String otherName = j.next();
                    if(!qqName.equals(otherName)){
                        context.write(new Text(qqName), new Text(otherName));
                    }
                }
            }
        }
    }
}
