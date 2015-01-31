package org.petuum.lda.training;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.log4j.Logger;


public class CluewebReducer implements Reducer<Text, Text, Text, Text> {
	
	private static Logger logger = Logger.getLogger(CluewebReducer.class);
	private static Text value = new Text();
	private MultipleOutputs mos;
	public void configure(JobConf conf) {
		 mos = new MultipleOutputs(conf);		
	}

	public void close() throws IOException {
		mos.close();
	}

	public void reduce(Text key, Iterator<Text> arg1,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//		StringBuffer buffer = new StringBuffer();
		int frequency=0;
		
		while(arg1.hasNext()){
			frequency++;
//			buffer.append(arg1.next().toString());
//			buffer.append(" ");
		}
		String newKey = frequency + " " + key.toString();
		System.out.println(frequency);
		mos.getCollector("vocab", reporter).collect(key, new Text(String.valueOf(frequency)));
		key.set(newKey);
//		value.set(buffer.toString());
//		mos.getCollector("index", reporter).collect(key, value);
		
		
	}
	
}
