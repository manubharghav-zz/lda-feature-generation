package org.petuum.lda.training;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;


public class CluewebCombiner implements Reducer<Text, IntWritable, Text, IntWritable> {
	
	private static Logger logger = Logger.getLogger(CluewebCombiner.class);
	private static IntWritable intwritableObject =new IntWritable();
	public void configure(JobConf conf) {
		System.out.println("Using the combiner");
		logger.info("Using the combiner");
	}

	public void close() throws IOException {
		
	}
	
	public void reduce(Text key, Iterator<IntWritable> arg1,
			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		int frequency=0;
		
		while(arg1.hasNext()){
			IntWritable w = arg1.next();
			frequency=frequency+w.get();
		}
		intwritableObject.set(frequency);
		output.collect(key, intwritableObject);		
	}
	
}
