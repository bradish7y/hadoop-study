package com.xzm.hadoop.unit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

public class MockTest {

	@Test
	public void mapTest() throws IOException, InterruptedException {
		WordCount.Map driver = new WordCount.Map();
		LongWritable key = new LongWritable(1);
		Text value = new Text("lion lion");
		Mapper.Context context = mock(Mapper.Context.class);

		driver.map(key, value, context);
		verify(context).write(new Text("lion"), new IntWritable(1));
	}

	@Test
	public void reduceTest() throws IOException, InterruptedException {
		WordCount.Reduce driver = new WordCount.Reduce();
		Text key = new Text("lion");
		Iterable<IntWritable> values = new Iterable<IntWritable>() {
			public Iterator<IntWritable> iterator() {
				List<IntWritable> l = new ArrayList<IntWritable>();
				l.add(new IntWritable(1));
				return l.iterator();
			}
		};
		Reducer.Context context = mock(Reducer.Context.class);
		driver.reduce(key, values, context);
		verify(context).write(new Text("lion"), new IntWritable(1));
	}
}
