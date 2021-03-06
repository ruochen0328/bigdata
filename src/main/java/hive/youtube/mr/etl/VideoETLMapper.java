package hive.youtube.mr.etl;

import java.io.IOException;

import hive.youtube.util.ETLUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VideoETLMapper extends Mapper<Object, Text, NullWritable, Text>{
	Text text = new Text();
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String etlString = ETLUtil.oriString2ETLString(value.toString());
		if(StringUtils.isBlank(etlString)) return;
		text.set(etlString);
		context.write(NullWritable.get(), text);
	}
	

}
