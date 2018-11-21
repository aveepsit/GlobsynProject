package org;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MBAMapper extends  Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text itemset = new Text();
	
	Text keyEmit = new Text("Total Transactions");
	IntWritable valEmit = new IntWritable();
	int partialSum = 0;
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException ,InterruptedException{
		
		partialSum++;
		
		String transaction = value.toString();
		List<String> itemsets = getItemSets(transaction.split(" "));
		
		for(String itmset : itemsets){				
			itemset.set(itmset.replaceAll(" ", ""));
			context.write(itemset, new IntWritable(1));
		}			
	}
	
@Override
	protected void cleanup(Context context) 
		throws IOException,InterruptedException {
	
			valEmit.set(partialSum);

			context.write(keyEmit, valEmit);

	}

private static List<String> getItemSets(String[] items) {
		
		List<String> itemsets = new ArrayList<String>();
		int n = items.length;		
		int[] masks = new int[n];
				
		for (int i = 0; i < n; i++)
			masks[i] = (1<<i);
		
		for (int i = 0; i < (1 << n); i++){				
			List<String> newList = new ArrayList<String>(n);
			for (int j = 0; j < n; j++){
				if ((masks[j] & i) != 0){      	
					newList.add(items[j]);
		        }
		        if(j == n-1 && newList.size() > 0 && newList.size() < 8){
		        	itemsets.add(newList.toString());
		        }
			}
		}	        			
		return itemsets;
	}
}
