package org.petuum.lda.training;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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
import org.tartarus.snowball.ext.englishStemmer;


public class ClueWebMapper extends Configured implements
Mapper<Writable, WritableWarcRecord, Text, Text> {
	private IntWritable val = new IntWritable(1);
	private static StopWordFilter filter;
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private Text stopword = new Text("STOPWORD");
	private Text newline = new Text("NEWLINE");
	private static Logger logger = Logger.getLogger(ClueWebMapper.class);
	englishStemmer stemmer = new englishStemmer();
	public void map(Writable key, WritableWarcRecord value, OutputCollector<Text, Text> output,
			Reporter arg3) throws IOException {
		WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(value.getRecord());
		String stemmedWord;
		try {
			String trecId = htmlRecord.getTargetTrecID();
			String parseContent = Jsoup.parse(htmlRecord.getRawRecord().getContentUTF8()).text().toLowerCase();
			String[] splits = parseContent.split(" ");
			splits = StringUtils.stripAll(splits, "&-:\\?><\\\" '#@*(),%. \\/");
			for(int i=0;i<splits.length;i++){
				if(!filter.isStopWord(splits[i])){
					if(splits[i].length()<2 || !StringUtils.isAlpha(splits[i]) ){
						continue;
					}
					stemmer.setCurrent(splits[i]);
					if(stemmer.stem()){
						stemmedWord = stemmer.getCurrent();
					}
					else{
						stemmedWord  = splits[i];
					}
										
					outputValue.set(trecId +" " + splits[i] + " "+i);
					outputKey.set(stemmedWord);
					output.collect(outputKey,outputValue );
				}
			}
		}
		catch(Exception e){
			System.out.println("exception occured while processing key:" + key.toString());
		}
		
	}
	public void configure(JobConf job) {
		
		filter= new StopWordFilter();
		
	}
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
