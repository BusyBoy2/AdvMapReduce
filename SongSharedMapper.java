import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SongSharedMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
	private IntWritable HeardFully;
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("\\|");
		if (parts[3].equalsIgnoreCase("1"))
		{
			HeardFully = new IntWritable(new Integer(parts[3]));	
			context.write(NullWritable.get(),HeardFully);
		}		
	
	}
}
