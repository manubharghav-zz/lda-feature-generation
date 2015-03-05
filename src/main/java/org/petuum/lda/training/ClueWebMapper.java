package org.petuum.lda.training;

import java.io.IOException;
import java.util.HashMap;

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
Mapper<Writable, WritableWarcRecord, Text, IntWritable> {
	private IntWritable one = new IntWritable(1);
	private Text text = new Text();
	private static StopWordFilter filter;
	public static final Log logger = LogFactory.getLog(ClueWebMapper.class);
	
	private Morphology morphAnalyzer;
	public void map(Writable key, WritableWarcRecord value, OutputCollector<Text, IntWritable> output,
			Reporter arg3) throws IOException {
		WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(value.getRecord());
		String stemmedWord=null;
		try {
			String trecId = htmlRecord.getTargetTrecID();
			logger.info("Map: Processing trecID: "+trecId + "  url:" +htmlRecord.getTargetURI());
			String parseContent = Jsoup.parse(htmlRecord.getRawRecord().getContentUTF8()).text().toLowerCase();
			String[] splits = parseContent.split(" ");
			splits = StringUtils.stripAll(splits, "&-:\\?><\\\" '#@*(),%. \\/");
			for(int i=0;i<splits.length;i++){
				if(!filter.isStopWord(splits[i])){
					if(splits[i].length()<2 || !StringUtils.isAlpha(splits[i]) ){
						continue;
					}
					stemmedWord = morphAnalyzer.stem(splits[i]);
					text.set(stemmedWord);
					output.collect(text, one);
				}
			}
		}
		catch(Exception e){
			System.out.println("exception occured while processing key:" + key.toString());
		}
		
	}
	public void configure(JobConf job) {
		
		filter= new StopWordFilter();
		morphAnalyzer = new Morphology();
		
	}
	public void close() throws IOException {		
	}

}
