import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SongSharedReducer extends Reducer<NullWritable, IntWritable, NullWritable, Text> {
	private int Count = 0; 
	private Text Total ;
	@Override
	public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException , InterruptedException
	{
		
		for (IntWritable value : values) {
			Count += value.get();
		}
		Total = new Text("Number of time song was shared is " + Integer.toString(Count));
		context.write(NullWritable.get(), Total);
	}

}
