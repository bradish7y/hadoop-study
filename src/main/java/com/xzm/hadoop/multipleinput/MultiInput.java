/**  
 * Copyright (c) 2014, xingzm@tydic.com All Rights Reserved.
 */
package com.xzm.hadoop.multipleinput;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * TODO(用几句话描述这个类型)<br/>
 * 
 * @author Bradish7Y
 * @since JDK 1.7
 * 
 */
public class MultiInput extends Configured implements Tool {
	static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] token = value.toString().split(" ");
			for (String s : token) {
				context.write(new Text(s), new IntWritable(1));
			}
		}

	}

	static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String[] token = value.toString().split(",");
			for (String s : token) {
				context.write(new Text(s), new IntWritable(1));
			}
		}

	}

	static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : value) {
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(MultiInput.class);
		job.setJobName("MultiInput test");

		// job.setMapperClass(Map1.class);

		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);

		Path out = new Path(args[2]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(out)) {
			System.out.println("************delete path=" + out.toString() + "*********");

			fs.delete(out, true);
		}

		// FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, out);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MultiInput(), args);
	}
}
