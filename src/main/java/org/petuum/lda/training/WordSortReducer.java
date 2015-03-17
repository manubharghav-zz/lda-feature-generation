package org.petuum.lda.training;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordSortReducer implements
		Reducer<DoubleWritable, Text, Text, DoubleWritable> {

	@Override
	public void configure(JobConf arg0) {
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(DoubleWritable arg0, Iterator<Text> arg1,
			OutputCollector<Text, DoubleWritable> arg2, Reporter arg3)
			throws IOException {
		while (arg1.hasNext()) {
			arg2.collect(arg1.next(), arg0);
		}

	}
}