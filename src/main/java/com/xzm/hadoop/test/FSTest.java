/**  
 * Copyright (c) 2014, xingzm@tydic.com All Rights Reserved.
 */
package com.xzm.hadoop.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * TODO(用几句话描述这个类型)<br/>
 * 
 * @author Bradish7Y
 * @since JDK 1.7
 * 
 */
public class FSTest {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String path = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		FSDataInputStream in = fs.open(new Path(path));

		Text line = new Text();
		LineReader lr = new LineReader(in);
		while (lr.readLine(line) != 0) {
			System.out.println(line);
			// line.clear();
		}
	}

}
