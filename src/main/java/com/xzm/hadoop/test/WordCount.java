package com.xzm.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		enum BadRecord {
			BAD
		};

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String line = value.toString();
			String[] token = line.split(" ");
			for (String s : token) {
				word.set(s);
				if (System.currentTimeMillis() % 2 == 0) {
					context.getCounter(BadRecord.BAD).increment(1);
					context.setStatus("Here not have error,just test!!!!!!!!!!!!!!!!");
				}
				context.write(word, one);
			}
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		try {
			ToolRunner.run(new WordCount(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		GenericOptionsParser go = new GenericOptionsParser(conf, args);

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : -1;
	}

}
