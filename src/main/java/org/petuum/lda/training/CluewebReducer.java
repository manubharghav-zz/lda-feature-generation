package org.petuum.lda.training;

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


public class CluewebReducer implements Reducer<Text, IntWritable, Text, Text> {
	
	private static Logger logger = Logger.getLogger(CluewebReducer.class);
	private static Text value = new Text();
	public void configure(JobConf conf) {
		System.out.println("Started Reduce");		
	}

	public void close() throws IOException {
	}

	public void reduce(Text key, Iterator<IntWritable> arg1,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int TFfrequency=0;
		int DFFrequency=0;
		int termFrequency=0;
		int DocFrequency=0;
		while(arg1.hasNext()){
			IntWritable w = arg1.next();
			if(w.get()>0){
				termFrequency+=w.get();
			}
			else{
				DocFrequency+=w.get();
			}
		}
		DocFrequency = DocFrequency*-1;
		double score = (float)termFrequency* (Math.log((float) 733018418 / DocFrequency));
		output.collect(key, new Text(String.valueOf(score)));		
	}
	
}
