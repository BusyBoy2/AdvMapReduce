import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SongHeardFully {
	public static void main(String[] args) throws Exception
	{
		//check if valid set of arguments is passed
				if(args.length < 2)
				{
					System.err.println("Song Heard- Error  - Arguments are not properly passed.");
					System.exit(-1);
				}
				//get configuration details and create a job
				Configuration conf = new Configuration();
				Job job = new Job(conf);
				//set jar class
				job.setJarByClass(SongHeardFully.class);

				//Creating Filesystem object with the configuration	
				FileSystem fs = FileSystem.get(conf);
				/*Check if output path (args[1])exist or not*/
				if(fs.exists(new Path(args[1]))){
				   /*If exist delete the output path*/
				   fs.delete(new Path(args[1]),true);
				}
				
				//set input path and output path
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				Path outputPath = new Path(args[1]);
				FileOutputFormat.setOutputPath(job, outputPath);
				job.setNumReduceTasks(1);
				
				//set mapper and reducer class
				job.setMapperClass(SongHeardFullyMapper.class);
				job.setReducerClass(SongHeardFullyReducer.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(IntWritable.class);
				
				   //execute the job
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
