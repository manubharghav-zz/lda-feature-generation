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
	private IntWritable val = new IntWritable(1);
	private static StopWordFilter filter;
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private Text stopword = new Text("STOPWORD");
	private Text newline = new Text("NEWLINE");
	public static final Log logger = LogFactory.getLog(ClueWebMapper.class);
	
	private Morphology morphAnalyzer;
	public void map(Writable key, WritableWarcRecord value, OutputCollector<Text, IntWritable> output,
			Reporter arg3) throws IOException {
		WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(value.getRecord());
		String stemmedWord;
		HashMap<String, Integer> map  = new HashMap<String, Integer>();
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
//					outputValue.set(trecId +" " + splits[i] + " "+i);
//					outputKey.set(stemmedWord);
					Integer count = map.get(stemmedWord);
					if(count==null){
						map.put(stemmedWord, 1);
					}
					else{
						map.put(stemmedWord,count+1);
					}
					if(map.size()>1000){
						publishMap(map, output);
					}
				}
			}
			
			publishMap(map, output);
		}
		catch(Exception e){
			System.out.println("exception occured while processing key:" + key.toString());
		}
		
	}
	
	void publishMap(HashMap<String, Integer> map,OutputCollector<Text, IntWritable> output) throws IOException{
		IntWritable mapValue = new IntWritable(0);
		Text key = new Text();
		for(String key1:map.keySet()){
			key.set(key1);
			mapValue.set(map.get(key1));
			output.collect(key,mapValue);
		}
		map.clear();
	}
	public void configure(JobConf job) {
		
		filter= new StopWordFilter();
		morphAnalyzer = new Morphology();
		
	}
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
