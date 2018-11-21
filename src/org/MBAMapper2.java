package org;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MBAMapper2 extends  Mapper<LongWritable, Text, Text, Text> {
	
	private Text outkey=new Text(); 
	private Text outvalue=new Text(); 
	private Text okey=new Text();
	private Text osub=new Text();
	private static List<String> getItemSets(String[] items) {
	
	List<String> itemsets = new ArrayList<String>();	
			
	int n = items.length;
			
	int[] masks = new int[n];
			
	for (int i = 0; i < n; i++) {
		masks[i] = (1<<i);
	}
		
			
	for (int i = 0; i < (1 << n); i++){				
				
		List<String> newList = new ArrayList<String>(n);
				
		for (int j = 0; j < n; j++){
		
			if ((masks[j] & i) != 0){      	
				newList.add(items[j]);
	        }
	                
	        if(j == n-1 && newList.size() > 0 && newList.size() < 5){
	        	itemsets.add(newList.toString());
	        }
		}
	}
	        			
	return itemsets;
}
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
	    	if(line !=null){
			
			String items[]=line.split("\\t");
			if(!items[0].startsWith("T")) {
				items[0]=items[0].replaceAll(" ", "");
				String out="0;"+items[1].trim();
				String keyout=items[0];
				String count=items[1];
				String parts[]=items[0].replace("[","").replace("]", "").split(",");
				outkey.set(keyout.replaceAll(" ", ""));
				outvalue.set(out);
				context.write(outkey, outvalue);
				List<String> list=getItemSets(parts);
				for(String item:list){
					String sub = item;
					String s=keyout+";"+count;
					okey.set(s);
					osub.set(sub.replaceAll(" ", ""));
					context.write(osub, okey);
				}
			}
			else {
				context.write(new Text(items[0]), new Text(items[1]));
			}
					
		}			
	}
}