package org.petuum.lda.training;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public  class WordSortMapper implements
		Mapper<LongWritable, Text, IntWritable, Text> {
	
	private Text word = new Text();
	private static String tab = "\\t";

	@Override
	public void configure(JobConf arg0) {
		System.out.println("started mapper");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<IntWritable, Text> arg2, Reporter arg3)
			throws IOException {
		String[] splits = arg1.toString().split(tab);
		word.set(splits[0]);
		arg2.collect(new IntWritable(Integer.parseInt(splits[1])), word);

	}
}