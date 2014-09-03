package com.xzm.hadoop.multipleoutput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiOutput extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		enum BadRecord {
			BAD
		};

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private MultipleOutputs<Text, IntWritable> mos = null;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			mos.close();
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			mos = new MultipleOutputs<Text, IntWritable>(context);

		}

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String line = value.toString();
			String[] token = line.split(" ");
			for (String s : token) {
				word.set(s);
				/*
				 * if (System.currentTimeMillis() % 2 == 0) {
				 * context.getCounter(BadRecord.BAD).increment(1);
				 * context.setStatus("Here not have error,just test!!!!!!!!!!!!!!!!"
				 * );
				 * }
				 */
				// context.write(word, one);
				mos.write(word, one, word.toString() + "/part");
				// mos.write(word, one, word.toString() + "/");
				// 输出到指定文件夹下，但输出格式是/user/cluster/multioutput/america/-m-00000
				// 所以最好是在"/"后面给个文件名
			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		try {
			ToolRunner.run(new MultiOutput(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		// GenericOptionsParser go = new GenericOptionsParser(conf, args);

		Job job = new Job(conf, "MultiOutput");
		job.setJarByClass(MultiOutput.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : -1;
	}
}
