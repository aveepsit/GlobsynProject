package org;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MBADriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		Job job1 = Job.getInstance(conf);
		
		
		job1.setJarByClass(MBADriver.class);
		job1.setMapperClass(MBAMapper.class);
		job1.setReducerClass(MBAReducer.class);
		
		job1.setOutputKeyClass(Text.class);	
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job1,new Path("MBAdir")); 
		
		FileOutputFormat.setOutputPath(job1, new Path("MBAout1"));	
		
		boolean success = job1.waitForCompletion(true);
		
		if(success) {
			Job job2 = Job.getInstance(conf);
			
			
			job2.setJarByClass(MBADriver.class);
			job2.setMapperClass(MBAMapper2.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setOutputKeyClass(Text.class);	
			job2.setOutputValueClass(Text.class);
			
			job2.setNumReduceTasks(0);
			
			FileInputFormat.addInputPath(job2,new Path("MBAout1")); 
			
			FileOutputFormat.setOutputPath(job2, new Path("MBAout2"));	
		
			boolean success2 = job2.waitForCompletion(true);
			
			if(success2) {
				
				Job job3 = Job.getInstance(conf);
				
				job3.setJarByClass(MBADriver.class);
				job3.setMapperClass(MBAMapper3.class);
				
				job3.setMapOutputKeyClass(Text.class);
				job3.setMapOutputKeyClass(Text.class);
				job3.setOutputKeyClass(Text.class);	
				job3.setOutputValueClass(Text.class);
				
				job3.setNumReduceTasks(0);
				
				FileInputFormat.addInputPath(job3,new Path("MBAout2")); 
				
				FileOutputFormat.setOutputPath(job3, new Path("MBAout3"));	
			
				/*boolean success3 = */job3.waitForCompletion(true);
				
				/*if(success3) {
				
					Job job4 = Job.getInstance(conf);
					
					job4.setJarByClass(MBADriver.class);
					job4.setMapperClass(MBAMapper4.class);
				
					job4.setMapOutputKeyClass(Text.class);
					job4.setMapOutputKeyClass(Text.class);
					job4.setOutputKeyClass(Text.class);	
					job4.setOutputValueClass(Text.class);
				
					job4.setNumReduceTasks(0);
				
					FileInputFormat.addInputPath(job4,new Path("MBAout3")); 
				
					FileOutputFormat.setOutputPath(job4, new Path("MBAout4"));	
			
					job4.waitForCompletion(true);
				}*/
			}
		}
	}

}
