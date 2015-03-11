package org.petuum.lda.preprocessing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;


public class PreProcessCombiner implements Reducer<Text, Text, Text, Text> {
	
	private static Logger logger = Logger.getLogger(PreProcessCombiner.class);
	public void configure(JobConf conf) {
		logger.info("Started Combiner");		
	}

	public void close() throws IOException {
	}

	public void reduce(Text key, Iterator<Text> arg1,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		StringBuffer buffer = new StringBuffer();
		buffer.append(arg1.next().toString());
		while(arg1.hasNext()){
			Text w = arg1.next();
			buffer.append(" " + w.toString());
		}
		output.collect(key, new Text(buffer.toString()));
	
	}
	
}
