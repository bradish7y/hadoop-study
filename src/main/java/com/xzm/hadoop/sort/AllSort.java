package com.xzm.hadoop.sort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AllSort extends Configured implements Tool {

	static class SortMap extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException,
				InterruptedException {
			context.write(key, value);
		}

	}

	static class SortReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = new Job(conf);

		job.setJarByClass(AllSort.class);
		job.setJobName("Sort");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// job.setMapperClass(SortMap.class);
		// job.setReducerClass(SortReduce.class);

		job.setPartitionerClass(TotalOrderPartitioner.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(3);

		RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.9, 10000,
				10);

		Path input = FileInputFormat.getInputPaths(job)[0];
		input = input.makeQualified(input.getFileSystem(conf));
		Path partitionFile = new Path(input, "_partitions");
		/*
		 * make sure that
		 * use TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
		 * partitionFile);
		 * not use
		 * TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		 */

		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
		InputSampler.writePartitionFile(job, sampler);

		// set paratition
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AllSort(), args);
	}

}
