package com.xzm.hadoop.template;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobTemplate extends Configured implements Tool {

	public static class M extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text values, Context context) throws IOException,
				InterruptedException {

		}
	}

	public static class R extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static class P extends Partitioner<Text, LongWritable> {
		@Override
		public int getPartition(Text key, LongWritable value, int parts) {
			int hash = key.toString().hashCode();
			return (hash & Integer.MAX_VALUE) % parts;
		}
	}

	public static class G implements RawComparator<Text> {
		public int compare(Text o1, Text o2) {
			return 0;
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return 0;
		}
	}

	public static class C implements RawComparator<Text> {
		public int compare(Text o1, Text o2) {
			return 0;
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return 0;
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Template this is!");
		job.setJarByClass(JobTemplate.class);

		job.setMapperClass(M.class);
		// job.setCombinerClass(R.class);
		job.setReducerClass(R.class);

		// job.setPartitionerClass(P.class);
		// job.setGroupingComparatorClass(G.class);
		// job.setSortComparatorClass(C.class);

		FileInputFormat.addInputPaths(job, args[0]);

		// job.setInputFormatClass(LzoTextInputFormat.class);
		// LzoTextInputFormat.addInputPaths(job, args[0]);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// job.setOutputFormatClass(TextOutputFormat.class);
		// TextOutputFormat.setOutputPath(job, new Path(args[1]));
		// TextOutputFormat.setCompressOutput(job, true);
		// TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		// SequenceFileOutputFormat.setCompressOutput(job, true);
		// SequenceFileOutputFormat.setOutputCompressorClass(job,
		// GzipCodec.class);
		// SequenceFileOutputFormat.setOutputCompressionType(job,
		// CompressionType.BLOCK);

		boolean successful = job.waitForCompletion(true);

		System.out.println(job.getJobID() + (successful ? " :successful" : " :failed"));

		return successful ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new JobTemplate(), args));
	}

}