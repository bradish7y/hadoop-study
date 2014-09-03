package com.xzm.hadoop.selfwritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, NullWritable, PersonWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
			String line = value.toString();
			String[] token = line.split(",");
			PersonWritable p = new PersonWritable(new IntWritable(Integer.valueOf(token[0])),
					new Text(token[1]), new Text(token[2]), new Text(token[3]));

			context.write(NullWritable.get(), p);

		}

	}

	public static class Reduce extends Reducer<NullWritable, PersonWritable, NullWritable, Text> {

		public void reduce(NullWritable key, Iterable<PersonWritable> values, Context context)
				throws IOException, InterruptedException {

			for (PersonWritable p : values) {
				context.write(key, new Text(p.toString()));
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
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
		// Set the key class for the map output data.
		job.setMapOutputKeyClass(NullWritable.class);
		// Set the value class for the map output data.
		job.setMapOutputValueClass(PersonWritable.class);

		// Set the key class for the job output data.
		job.setOutputKeyClass(NullWritable.class);
		// Set the value class for the job output data.
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
