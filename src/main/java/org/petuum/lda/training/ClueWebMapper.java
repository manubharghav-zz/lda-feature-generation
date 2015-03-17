package org.petuum.lda.training;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.*;
import org.jsoup.Jsoup;

import edu.stanford.nlp.process.Morphology;


public class ClueWebMapper extends Configured implements
Mapper<Writable, Text, Text, IntWritable> {
	private IntWritable one = new IntWritable(1);
	private IntWritable docOne = new IntWritable(-1);
	private Text text = new Text();
	public static final Log logger = LogFactory.getLog(ClueWebMapper.class);
	public void map(Writable key, Text value, OutputCollector<Text, IntWritable> output,
			Reporter arg3) throws IOException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		logger.info("Processing document : "+tokenizer.nextToken());
		while(tokenizer.hasMoreTokens()){
			text.set(tokenizer.nextToken());
			one.set(Integer.parseInt(tokenizer.nextToken()));
			output.collect(text, one);
			output.collect(text, docOne);
		}
		
	}
	public void configure(JobConf job) {
		
	}
	public void close() throws IOException {		
	}

}
