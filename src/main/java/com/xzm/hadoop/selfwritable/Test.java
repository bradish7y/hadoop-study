package com.xzm.hadoop.selfwritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test extends Configured implements Tool {
	public static class Map extends
			Mapper<LongWritable, Text, CustomWritable, NullWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] token = line.split(",");

			System.out.println("toke[0]=" + token[0]);
			CustomWritable p = new CustomWritable(new IntWritable(
					Integer.valueOf(token[0])), new Text(token[1]), new Text(
					token[2]), new Text(token[3]));

			context.write(p, NullWritable.get());

		}

	}

	public static class Reduce extends
			Reducer<CustomWritable, NullWritable, CustomWritable, NullWritable> {

		public void reduce(CustomWritable key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (NullWritable e : values) {
				sum++;
				context.write(key, e);
			}
			System.out.println("sum=" + sum);
		}
	}

	public static class P extends Partitioner<CustomWritable, NullWritable> {
		@Override
		public int getPartition(CustomWritable key, NullWritable value,
				int parts) {
			int hash = key.getId().get();
			return (hash & Integer.MAX_VALUE) % parts;
		}
	}

	public static class G implements RawComparator<CustomWritable> {

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8,
					b2, s2, Integer.SIZE / 8);
		}

		public int compare(CustomWritable o1, CustomWritable o2) {
			int l = o1.getId().get();
			int r = o2.getId().get();
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		try {
			ToolRunner.run(new Test(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = new Job(conf, "Test SelfWritableComparable");
		job.setJarByClass(Test.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(P.class);// 相同key分配到同一个人去
		// 将key相同的分到一组,如果不配置，在分组的时候会找到默认的CustomWritable的CompareTo，进行分组（只有前2个字段相等才会是一组）
		job.setGroupingComparatorClass(G.class);

		// Set the key class for the map output data.
		job.setMapOutputKeyClass(CustomWritable.class);
		// Set the value class for the map output data.
		// job.setMapOutputValueClass(NullWritable.class);

		// Set the key class for the job output data.
		job.setOutputKeyClass(CustomWritable.class);
		// Set the value class for the job output data.
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(2);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
