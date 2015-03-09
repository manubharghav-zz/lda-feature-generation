package org.petuum.lda.preprocessing;

import java.io.IOException;
import java.util.Iterator;

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
	private MultipleOutputs mos;
	public void configure(JobConf conf) {
		System.out.println("Started Reduce");		
	}

	public void close() throws IOException {
	}

	public void reduce(Text key, Iterator<Text> arg1,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//		StringBuffer buffer = new StringBuffer();
		int frequency=0;
		StringBuffer buffer = new StringBuffer();
		int numString = 0;
		buffer.append(arg1.next().toString());
		while(arg1.hasNext()){
			Text w = arg1.next();
			buffer.append(" " + w.toString());
		}
		String newKey = frequency + " " + key.toString();
		output.collect(key, new Text(buffer.toString()));
		key.set(newKey);	
	}
	
}
