/**  
 * Copyright (c) 2014, xingzm@tydic.com All Rights Reserved.
 */
package com.xzm.hadoop.selfwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * for exmple:2014001013,ronaldo,Brazil,just test<br/>
 * 
 * @author Bradish7Y
 * @since JDK 1.7
 * 
 */
public class CustomWritable implements WritableComparable<CustomWritable> {
	private IntWritable id;
	private Text value1;
	private Text value2;
	private Text value3;

	public CustomWritable() {
		this(new IntWritable(), new Text(), new Text(), new Text());
	}

	public CustomWritable(IntWritable id, Text value1, Text value2, Text value3) {
		super();
		this.id = id;
		this.value1 = value1;
		this.value2 = value2;
		this.value3 = value3;
	}

	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		value1.readFields(in);
		value2.readFields(in);
		value3.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		id.write(out);
		value1.write(out);
		value2.write(out);
		value3.write(out);
	}

	public int compareTo(CustomWritable that) {

		int ret = this.id.compareTo(that.id);
		if (ret != 0) {
			return ret;
		}
		return this.value1.compareTo(that.value1);
	}

	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = id;
	}

	public Text getValue1() {
		return value1;
	}

	public void setValue1(Text value1) {
		this.value1 = value1;
	}

	public Text getValue2() {
		return value2;
	}

	public void setValue2(Text value2) {
		this.value2 = value2;
	}

	public Text getValue3() {
		return value3;
	}

	public void setValue3(Text value3) {
		this.value3 = value3;
	}

	@Override
	public String toString() {
		return "PersonWritable [id=" + id + ", value1=" + value1 + ", value2="
				+ value2 + ", value3=" + value3 + "]";
	}

}
