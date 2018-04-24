package crystallball;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StripesApproach {
	
	public static class Map extends Mapper<LongWritable, Text, Text, MyMapWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] arr = value.toString().split("\\s+");
			
			for (int i = 0; i < arr.length; i++) {
				
				MyMapWritable map = new MyMapWritable();
				for (int j = i + 1; j < arr.length && !arr[i].equals(arr[j]); j++) {
					Text k = new Text(arr[j]);
					if (map.containsKey(k)) {
						int v = Integer.parseInt(map.get(k).toString()) + 1;
						map.put(k, new IntWritable(v));
					} else {
						map.put(k, new IntWritable(1));
					}
				}
				
				context.write(new Text(arr[i]), map);
			}
		}		
	}
	
	public static class Reduce extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {

		@Override
		protected void reduce(Text key, Iterable<MyMapWritable> values, Context context) throws IOException, InterruptedException {
			
			MyMapWritable map = new MyMapWritable();
			int sum = 0;
			
			for (MyMapWritable val : values) {
				for (Entry<Writable, Writable> entry : val.entrySet()) {
					Text k1 = (Text) entry.getKey();
					IntWritable v1 = (IntWritable) entry.getValue();
					sum += v1.get();
					if (map.containsKey(k1)) {
						IntWritable vPrev = (IntWritable) map.get(k1);
						int newVal = vPrev.get() + v1.get();
						map.put(k1, new IntWritable(newVal));
					} else {
						map.put(k1, v1);
					}	
				}
			}
			
			MyMapWritable resultMap = new MyMapWritable();
			for (Entry<Writable, Writable> resultEntry : map.entrySet()) {
				Text resultKey = (Text) resultEntry.getKey();
				String str = resultEntry.getValue().toString();
				Text resultValue = new Text(str + "/" + sum);
				resultMap.put(resultKey, resultValue);
			}
			
			context.write(key, resultMap);
		}		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		Job job = new Job(conf, "StripesApproach");
		job.setJarByClass(StripesApproach.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyMapWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
