package org.petuum.lda.preprocessing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.log4j.Logger;


public class PreProcessReducer implements Reducer<Text, Text, Text, Text> {
	
	private static Logger logger = Logger.getLogger(PreProcessReducer.class);
	private static Text value = new Text();
	private Map<String, Integer> counts;
	private Integer freq;
	public void configure(JobConf conf) {
		System.out.println("Started Reduce");	
		counts = new HashMap<String,Integer>();
	}

	public void close() throws IOException {
	}

	public void reduce(Text key, Iterator<Text> arg1,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		StringBuffer buffer = new StringBuffer();
		buffer.append(arg1.next().toString());
		while(arg1.hasNext()){
			Text w = arg1.next();
			freq = counts.get(w.toString());
			if(freq == null){
				counts.put(w.toString(), 1);
			}
			else{
				counts.put(w.toString(), freq+1);
			}
			
		}
		
		for(String keyString:counts.keySet()){
			buffer.append(keyString).append(" ").append(counts.get(keyString)).append(" ");
		}
		counts.clear();
		output.collect(key, new Text(buffer.toString()));
	}
	
}
