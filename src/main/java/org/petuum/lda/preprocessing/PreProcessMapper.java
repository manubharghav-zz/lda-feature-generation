package org.petuum.lda.preprocessing;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import org.petuum.lda.training.StopWordFilter;
import org.petuum.lda.training.WarcHTMLResponseRecord;
import org.petuum.lda.training.WritableWarcRecord;

import edu.stanford.nlp.process.Morphology;

public class PreProcessMapper extends Configured implements
		Mapper<Writable, WritableWarcRecord, Text, Text> {
	private Text text = new Text();
	private Text outWord = new Text();
	private static StopWordFilter filter;
	public static final Log logger = LogFactory.getLog(PreProcessMapper.class);
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private Morphology morphAnalyzer;
	private HashMap<String, Integer> counts = new HashMap<String, Integer>();

	public void map(Writable key, WritableWarcRecord value,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {
		final WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(
				value.getRecord());
		String stemmedWord = null;
		try {
			String trecId = htmlRecord.getTargetTrecID();
			logger.info("Map: Processing trecID: " + trecId + "  url:"
					+ htmlRecord.getTargetURI());
			if(htmlRecord.getTargetURI().contains("innovative-dsp")){
				return;
			}
			class Task implements Callable<String> {
				@Override
				public String call() throws Exception {
					return Jsoup
							.parse(htmlRecord.getRawRecord().getContentUTF8())
							.text().toLowerCase();
				}
			}

			Future<String> future = executor.submit(new Task());

			String parseContent = future.get(60, TimeUnit.SECONDS);

			logger.info("Parsed Url: " + htmlRecord.getTargetURI());
			String[] splits = parseContent.split(" ");
			splits = StringUtils.stripAll(splits, "&-:\\?><\\\" '#@*(),%. \\/");
			for (int i = 0; i < splits.length; i++) {
				if (!filter.isStopWord(splits[i].toLowerCase())) {
					if (splits[i].length() < 2
							|| !StringUtils.isAlpha(splits[i])) {
						continue;
					}
					stemmedWord = morphAnalyzer.stem(splits[i]);
					Integer count = counts.get(stemmedWord);
					if(count == null){
						counts.put(stemmedWord, 1);
					}
					else{
						counts.put(stemmedWord, count+1);
					}
//					text.set(trecId);
//					outWord.set(stemmedWord);
//					output.collect(text, outWord);
				}
			}
			text.set(trecId);
			StringBuffer buffer = new StringBuffer();
			for (String stemWord:counts.keySet()){
				buffer.append(stemWord).append(" ").append(counts.get(stemWord)).append(" ");
			}
			
			counts.clear();
			
			output.collect(text, new Text(buffer.toString()));
		}  catch (InterruptedException e) {
			System.out.println("exception occured while processing key:"
					+ key.toString());
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			System.out.println("exception occured while processing key:"
					+ key.toString());
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			System.out.println("exception occured while processing key:"
					+ key.toString());
			System.out.println("Killing the executor and creatign a new one.");
			executor.shutdownNow();
			executor = Executors.newSingleThreadExecutor();
		}catch (Exception e) {
			System.out.println("exception occured while processing key:"
					+ key.toString());
		}

	}

	public void configure(JobConf job) {

		filter = new StopWordFilter();
		morphAnalyzer = new Morphology();

	}

	public void close() throws IOException {
		executor.shutdown();
	}

}
