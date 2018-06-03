import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueListenersMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
	private LongWritable UserId;
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("\\|");
		UserId = new LongWritable((new Integer(parts[0])));	
		context.write(UserId, NullWritable.get());
	}

}
