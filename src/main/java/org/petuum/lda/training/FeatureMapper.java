package org.petuum.lda.training;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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


public class FeatureMapper extends Configured implements
Mapper<Writable, WritableWarcRecord, Text, Text> {
	private static StopWordFilter filter;
	public static final Log logger = LogFactory.getLog(FeatureMapper.class);
	private Map<String, Integer> featureIdMap = new HashMap<String, Integer>();
	
	private Morphology morphAnalyzer;
	public void map(Writable key, WritableWarcRecord value, OutputCollector<Text, Text> output,
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
				if(!filter.isStopWord(splits[i].toLowerCase())){
					if(splits[i].length()<2 || !StringUtils.isAlpha(splits[i]) ){
						continue;
					}
					stemmedWord = morphAnalyzer.stem(splits[i]);										
					Integer count = map.get(stemmedWord);
					if(count==null){
						map.put(stemmedWord, 1);
					}
					else{
						map.put(stemmedWord,count+1);
					}
				}
			}
			publishMap(map, output,trecId);
			map.clear();
		}
		catch(Exception e){
			System.out.println("exception occured while processing key:" + key.toString());
		}
		
	}
	
	void publishMap(HashMap<String, Integer> map,OutputCollector<Text, Text> output, String trecId) throws IOException{
		StringBuffer buffer = new StringBuffer();
		for(String key1:map.keySet()){
			Integer featureId =  featureIdMap.get(key1);
			if(featureId!=null){
				buffer.append(featureIdMap.get(key1)).append(":").append(map.get(key1)).append("\t");
			}
			else{
				logger.info("No mappring found for "+key1);
			}
		}
		
		output.collect(new Text(trecId),new Text(buffer.toString()));
		
	}
	public void configure(JobConf job) {
		int vocabSize = job.getInt("VocabSize", 1000000);
		String vocabPath =job.get("VocabFile");
		System.out.println(vocabPath);
		BufferedReader br=null;
		try {
			int count = 1;
			FileSystem fs = FileSystem.get(job);

			logger.info("reading from path:" + vocabPath);

			br = new BufferedReader(new InputStreamReader(fs.open(new Path(
					vocabPath))));

			String line;
			while((line=br.readLine())!=null && count <= vocabSize){
				String[] splits = line.split("\\t");
				featureIdMap.put(splits[0],count);
				count++;
			}
			line = br.readLine();
			logger.info("loaded the vocab. Size of the vocab: " + count);
		}
		catch(Exception e){
			System.out.println("Error loading vocab file from disk. ERROR" + e);
		}
		finally {
			// you should close out the BufferedReader
			try {
				br.close();
			} catch (Exception e) {

			}
		}
		
		filter= new StopWordFilter();
		morphAnalyzer = new Morphology();
		
	}
	public void close() throws IOException {
		
		
	}

}
