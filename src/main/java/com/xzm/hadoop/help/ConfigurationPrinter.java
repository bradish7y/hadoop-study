package com.xzm.hadoop.help;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {

	static {
		Configuration.addDefaultResource("core-default.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("hdfs-default.xml");
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		for (Entry<String, String> e : conf) {
			System.out.println("name=" + e.getKey() + ",value=" + e.getValue());
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		/*
		 * hadoop jar hadoop.jar package.main -D color=green
		 */
		System.out.println(ToolRunner.run(new ConfigurationPrinter(), args));
	}

}
