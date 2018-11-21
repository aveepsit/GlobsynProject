package org;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MBAReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable outvalue = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			
			throws IOException, InterruptedException {
			Iterator<IntWritable> itr = values.iterator();	
			int sum = 0;
			while(itr.hasNext()) {
				IntWritable val = itr.next();
				int x = val.get();
				sum = sum+x;
			}
			outvalue.set(sum);
			context.write(key, outvalue);
	}
}
