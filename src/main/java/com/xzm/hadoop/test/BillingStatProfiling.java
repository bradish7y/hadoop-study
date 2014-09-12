package com.xzm.hadoop.test;

import java.io.IOException;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BillingStatProfiling extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		enum Profiling {
			LESS10MS, _10TO20MS, _20TO30MS, _30TO40MS, _40TO50MS, _50TO60MS, _60TO70MS, _70TO80MS, _80TO90MS, _90TO100MS, _100TO110MS, _110MSTO150MS, _150TO1000MS, _1000TO2000MS, _2000TO5000MS, _5000TO50000MS, GREATER50000MS
		};

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {

			String line = value.toString();
			int index = line.lastIndexOf("返回统一接入,耗时");
			if (index == -1) {
				return;
			}

			String s[] = line.split("返回统一接入,耗时");
			int endIndex = s[1].indexOf("ms");
			int costTime = Integer.valueOf(s[1].substring(0, endIndex).trim());
			if (costTime < 10) {
				word.set(Profiling.LESS10MS.toString());
			} else if (costTime >= 10 && costTime < 20) {
				word.set(Profiling._10TO20MS.toString());
			} else if (costTime >= 20 && costTime < 30) {
				word.set(Profiling._20TO30MS.toString());
			} else if (costTime >= 30 && costTime < 40) {
				word.set(Profiling._30TO40MS.toString());
			} else if (costTime >= 40 && costTime < 50) {
				word.set(Profiling._40TO50MS.toString());
			} else if (costTime >= 50 && costTime < 60) {
				word.set(Profiling._50TO60MS.toString());
			} else if (costTime >= 60 && costTime < 70) {
				word.set(Profiling._60TO70MS.toString());
			} else if (costTime >= 70 && costTime < 80) {
				word.set(Profiling._70TO80MS.toString());
			} else if (costTime >= 80 && costTime < 90) {
				word.set(Profiling._80TO90MS.toString());
			} else if (costTime >= 90 && costTime < 100) {
				word.set(Profiling._90TO100MS.toString());
			} else if (costTime >= 100 && costTime < 110) {
				word.set(Profiling._100TO110MS.toString());
			} else if (costTime >= 110 && costTime < 150) {
				word.set(Profiling._110MSTO150MS.toString());
			} else if (costTime >= 150 && costTime < 1000) {
				word.set(Profiling._150TO1000MS.toString());
			} else if (costTime >= 1000 && costTime < 2000) {
				word.set(Profiling._1000TO2000MS.toString());
			} else if (costTime >= 2000 && costTime < 5000) {
				word.set(Profiling._2000TO5000MS.toString());
			} else if (costTime >= 5000 && costTime < 50000) {
				word.set(Profiling._5000TO50000MS.toString());
			} else {
				word.set(Profiling.GREATER50000MS.toString());
			}

			context.write(word, one);

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		// reuse
		private IntWritable counts = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}
			counts.set(sum);
			context.write(key, counts);
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		try {
			ToolRunner.run(new BillingStatProfiling(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public int run(String[] args) throws Exception {
		// GenericOptionsParser go = new GenericOptionsParser(conf, args);

		Job job = new Job(getConf(), "BillingStatProfiling");
		job.setJarByClass(BillingStatProfiling.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		return job.waitForCompletion(true) ? 0 : -1;
	}
}
