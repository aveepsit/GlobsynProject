package org;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MBAMapper3 extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	private int tno;
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException ,InterruptedException{

		String line = value.toString();
		
		if(line!=null){
			
			String items[]=line.split("\\t");
			if (items[0].startsWith("T")) {
				tno = Integer.parseInt(items[1]);
			}
			if(!items[0].startsWith("T")) {
				String lhs = items[0];
				String rhs = getRHS(items,tno);
			
				outkey.set(lhs);
				outvalue.set(rhs);
			
				context.write(outkey, outvalue);
			}
			/*else {
				context.write(new Text(items[0]), new Text(items[1]));
			}*/
		}
	}

	private String getRHS(String[] items, int tno) {
		String word0 = items[0];
		String word1 = items[1];
		String p = "";
		
		if(!word1.startsWith("0;")){
			
			String item0 = word0.replace("[","").replace("]"," ").replace(",", " ").trim();
			String item1 = word1.replace("[","").replace("]"," ").replace(",", " ").trim();
			
			String[] modword0 = item0.split(" ");
			String[] modword1 = item1.split(" ");
			
			Set<String> s1 = new HashSet<String>();
			
			for(String x : modword0)
				s1.add(x);
				
			for(String x : modword1)
			{
				if(!s1.contains(x))
					p = p+" "+x;
			}
			String out = p.replaceFirst(" ", "[").replace(" ", ",");
			String finOut = out.replace(",;", "];").replace("[;", "0;");
			
			double sup;
			
			String[] support = finOut.split(";");
			int val = Integer.parseInt(support[1]);
			
			sup = (double)val/(double)tno;
			
			finOut = support[0]+";"+support[1]+";"+"Support = "+String.valueOf(sup);
			
			return finOut;
		}
		else
		{	
			//items[0] = [a,b,c]	items[1]= 0;1
			String finOut = items[1];
			
			double sup;
			
			String[] support = finOut.split(";");
			int val = Integer.parseInt(support[1]);
			
			sup = (double)val/(double)tno;
			
			finOut = support[1]+";"+"Support = "+String.valueOf(sup);
			
			return finOut;
		}
	}

}