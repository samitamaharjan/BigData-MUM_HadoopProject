package crystallball;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairsApproach {
	
	public static class Map extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] arr = value.toString().split("\\s+");
			
			for (int i = 0; i < arr.length; i++) {
				for (int j = i + 1; j < arr.length && !arr[i].equals(arr[j]); j++){
					Text k = new Text(arr[i]);
					Text v = new Text(arr[j]);
					Text asterisk = new Text("*");
					
					PairWritable pair1 = new PairWritable(k, asterisk);
					PairWritable pair2 = new PairWritable(k, v);
					
					context.write(pair1, new IntWritable(1));
					context.write(pair2, new IntWritable(1));
				}
			}
		}		
	}
	
	public static class Reduce extends Reducer<PairWritable, IntWritable, PairWritable, Text> {
		int n;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			n = 0;
		}

		@Override
		protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			if (key.getValue().toString().equals("*")) {
				n = sum;
			} else {
				String freq = String.format("%d/%d", sum, n);
				context.write(key, new Text(freq));
			}
		}	
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		Job job = new Job(conf, "PairApproach");
		job.setJarByClass(PairsApproach.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(PairWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}