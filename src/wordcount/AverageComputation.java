package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageComputation {
	
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] arr = value.toString().split("\\s+");
			String ip = arr[0];
			float time;
			
			if (arr[arr.length - 1].matches("[0-9]+")) {
				time = Float.parseFloat(arr[arr.length - 1]);
				context.write(new Text(ip), new FloatWritable(time));
			}
			
			/*if (arr[arr.length - 1] != "-") {
				time = Integer.parseInt(arr[arr.length - 1]);
				context.write(new Text(ip), new IntWritable(time));
			} */
		}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float sum = 0, count = 0;
			
			for (FloatWritable val : values) {
				sum += val.get();
				count++;
			}
			
			float average = sum / count;
			context.write(key, new FloatWritable(average));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		/*Check if output path (args[1])exist or not*/
		if(fs.exists(new Path(args[1]))){
		 /*If exist delete the output path*/
		 fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "AverageComputation");
		job.setJarByClass(AverageComputation.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}	
}