package wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AverageComputationInmapper {
	
	public static class Map extends Mapper<LongWritable, Text, Text, PairWritable> {
		
		HashMap<String, PairWritable> hashMap;
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			hashMap = new HashMap<String, PairWritable>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] arr = value.toString().split("\\s+");
			String ip = arr[0];
			float time = 0;
			int count = 1;
			if (arr[arr.length - 1].matches("[0-9]+")) {
				time = Integer.parseInt(arr[arr.length - 1]);
				if (hashMap.containsKey(ip)) {
					time += hashMap.get(ip).getKey();
					count += hashMap.get(ip).getValue();
					hashMap.put(ip, new PairWritable(time, count));
				} else {
					hashMap.put(ip, new PairWritable(time, count));
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(String ip : hashMap.keySet()){
				Text key = new Text(ip);
				context.write(new Text(key), hashMap.get(ip));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, PairWritable, Text, FloatWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
			float sum = 0; int count = 0;
			
			for (PairWritable val : values) {
				sum += val.getKey();
				count += val.getValue();
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

		Job job = new Job(conf, "AverageComputationInMapper");
		job.setJarByClass(AverageComputationInmapper.class);
		
		job.setMapOutputValueClass(PairWritable.class);
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

class PairWritable implements Writable{
	private Float key;
	private Integer value;
	
	public PairWritable(Float key, Integer value) {
		this.key = key;
		this.value = value;
	}
	
	public PairWritable() {
		// TODO Auto-generated constructor stub
	}

	public Float getKey() {
		return key;
	}

	public void setKey(Float key) {
		this.key = key;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readFloat();
		value = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(key);
		out.writeInt(value);
	}	
	
}