import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongHeardFullyReducer extends Reducer<NullWritable, IntWritable, NullWritable, Text> {
	
	private int Listened = 0; 
	private Text TotalCount ;
	@Override
	public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException , InterruptedException
	{
		
		for (IntWritable value : values) {
			Listened  += value.get();
		}
		TotalCount = new Text("Number of times a song was heard fully is " + Integer.toString(Listened));
		context.write(NullWritable.get(), TotalCount);
	}

}
