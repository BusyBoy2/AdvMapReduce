import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UniqueListenersReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
	
	@Override
	public void reduce(LongWritable key,Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
	{
		  context.write(key, NullWritable.get());
	}

}
