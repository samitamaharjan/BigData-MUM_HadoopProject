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

public class HybridApproach {
	
	public static class Map extends Mapper<LongWritable, Text, PairWritable, IntWritable> {
		MyMapWritable hashMap;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			hashMap = new MyMapWritable();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] arr = value.toString().split("\\s+");
			
			for (int i = 0; i < arr.length; i++) {
				for (int j = i + 1; j < arr.length && !arr[i].equals(arr[j]); j++){
					Text k = new Text(arr[i]);
					Text v = new Text(arr[j]);
										
					PairWritable pair = new PairWritable(k, v);
					
					if (hashMap.containsKey(pair)) {
						IntWritable v1 = (IntWritable) hashMap.get(pair);
						int v2 = v1.get() + 1;
						hashMap.put(pair, new IntWritable(v2));
					} else {
						hashMap.put(pair, new IntWritable(1));
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<Writable, Writable> entry : hashMap.entrySet()) {
				PairWritable key = (PairWritable) entry.getKey();
				IntWritable value = (IntWritable)entry.getValue();
				context.write(key, value);
				//System.out.println(key + " " + value.toString());
			}
		}
	}
	
	public static class Reduce extends Reducer<PairWritable, IntWritable, Text, MyMapWritable> {
		Text wPrev;
		MyMapWritable resultMap;
		MyMapWritable finalMap;
		int total;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			wPrev = null;
			resultMap = new MyMapWritable();
			finalMap = new MyMapWritable();
			total = 0;
		}

		@Override
		protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			Text w = new Text(key.getKey());
			if (wPrev != null && !w.toString().equals(wPrev.toString())) {
				for (Entry<Writable, Writable> entry : resultMap.entrySet()) {
					Text k1 = (Text) entry.getKey();
					IntWritable v1 = (IntWritable) entry.getValue();
					Text val = new Text(v1.toString() + "/" + total);
					finalMap.put(k1, val);
				}
				context.write(wPrev, finalMap);
				resultMap.clear();
				finalMap.clear();
				total = 0;
			} 
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			total += sum;
			resultMap.put(new Text(key.getValue()), new IntWritable(sum));
			wPrev = new Text(w);			
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<Writable, Writable> entry : resultMap.entrySet()) {
				Text k1 = (Text) entry.getKey();
				IntWritable v1 = (IntWritable) entry.getValue();
				Text val = new Text(v1.toString() + "/" + total);
				finalMap.put(k1, val);
			}
			context.write(wPrev, finalMap);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		Job job = new Job(conf, "HybridApproach");
		job.setJarByClass(HybridApproach.class);
		
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
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
