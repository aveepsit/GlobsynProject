package org;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MBAMapper4 extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException ,InterruptedException{

		String line = value.toString();
		int LHSno;
		if(line!=null){
			
			String items[]=line.split(";");
			if (items[1].startsWith("S")) {
				String[] itm = items[0].split("\\t");
				LHSno = Integer.parseInt(itm[1]);
			}
			else {
				context.write(new Text(items[0]), new Text(items[1]));
			}
		}
	}

}