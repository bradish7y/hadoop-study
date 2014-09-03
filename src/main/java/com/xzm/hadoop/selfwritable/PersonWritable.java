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
public class PersonWritable implements WritableComparable<PersonWritable> {
	private IntWritable id;
	private Text name;
	private Text country;
	private Text reserv;

	public PersonWritable() {
		this(new IntWritable(), new Text(), new Text(), new Text());
	}

	public PersonWritable(IntWritable id, Text name, Text country, Text reserv) {
		super();
		this.id = id;
		this.name = name;
		this.country = country;
		this.reserv = reserv;
	}

	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		name.readFields(in);
		country.readFields(in);
		reserv.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		id.write(out);
		name.write(out);
		country.write(out);
		reserv.write(out);
	}

	public int compareTo(PersonWritable that) {

		return this.id.compareTo(that.id);
	}

	@Override
	public String toString() {
		return "PersonWritable [id=" + id + ", name=" + name + ", country=" + country + ", reserv="
				+ reserv + "]";
	}

}
