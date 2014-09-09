package com.xzm.hadoop.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TestIterable {

	public static class M1 extends Mapper<Object, Text, Text, Text> {
		private Text oKey = new Text();
		private Text oVal = new Text();
		String[] lineArr;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			lineArr = value.toString().split(" ");
			oKey.set(lineArr[0]);
			oVal.set(lineArr[1]);
			context.write(oKey, oVal);
		}
	}

	public static class R1 extends Reducer<Text, Text, Text, Text> {
		List<String> valList = new ArrayList<String>();
		List<Text> textList = new ArrayList<Text>();
		String strAdd;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// valList.clear();
			// textList.clear();
			strAdd = "";
			for (Text val : values) {
				valList.add(val.toString());// ok
				textList.add(val);// 这种输出有问题,因为类型重用问题，所以Text始终是一个对象
				// textList.add(new Text(val));// 修改方法：textList.add(new
				// Text(val))新建对象
			}

			// error
			for (Text text : textList) {
				strAdd += text.toString() + ", ";
			}
			System.out.println(key.toString() + "\t" + strAdd);
			System.out.println(".......................");

			// ok
			strAdd = "";
			for (String val : valList) {
				strAdd += val + ", ";
			}
			System.out.println(key.toString() + "\t" + strAdd);
			System.out.println("----------------------");

			// error jvm复用，所以第2次调用时，values为空
			valList.clear();
			strAdd = "";
			for (Text val : values) {
				valList.add(val.toString());
			}
			for (String val : valList) {
				strAdd += val + ", ";
			}
			System.out.println(key.toString() + "\t" + strAdd);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>");
		}
	}

	public static class R2 extends Reducer<Text, Text, Text, Text> {
		List<String> valList = new ArrayList<String>();
		List<Text> textList = new ArrayList<Text>();
		String strAdd;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(key, t);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		System.out.println("------------------------");
		Job job = new Job(conf, "TestIterable");
		job.setJarByClass(TestIterable.class);
		job.setMapperClass(M1.class);
		job.setCombinerClass(R2.class);
		job.setReducerClass(R1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 输入输出路径
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileSystem fs = FileSystem.get(conf);
		// if (fs.exists(new Path(otherArgs[1]))) {
		// fs.delete(new Path(otherArgs[1]), true);
		// }
		// FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
