package org.petuum.lda.training;


import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;



public class FeatureFileGenerator extends Configured {
	private static Logger logger = Logger.getLogger(FeatureFileGenerator.class);
	private FileSystem fs;
	public FeatureFileGenerator(Configuration conf){
		super(conf);
	}
	public static void main(String[] args) throws IOException {
		String input = args[0];
		String output=args[1];
		String vocabFile = args[2];
		int vocabSize = Integer.parseInt(args[3]);
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);
		Configuration conf = new Configuration();
		FeatureFileGenerator job = new FeatureFileGenerator(conf);
		job.generateFeatures(inputPath, outputPath, vocabFile, vocabSize);
	}
	
	public void generateFeatures(Path input, Path output, String vocabFile, int vocabSize) throws IOException {
		logger.info("Parsing Data from " + input.toString());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		JobConf job = new JobConf();
		job.setJobName("Feature File Generation Job");
		fs = FileSystem.get(job);
		
		
		Path FeatureFilesLocation = new Path(output, "featurefiles");
		fs.delete(FeatureFilesLocation, true);
		
		Path MergedFeatureFilesLocation = new Path(output, "mergedFeatureFiles");
		
		job.setJarByClass(FeatureFileGenerator.class);

		job.set("VocabFile", vocabFile);
		job.setInt("VocabSize", vocabSize);
		
		job.setInputFormat(WarcFileInputFormat.class);
		// adding inputs.
	    List<Path> inputhPaths = new ArrayList<Path>();
        FileSystem fs = FileSystem.get(job);
        FileStatus[] listStatus = fs.globStatus(input);
        for (FileStatus fstat : listStatus) {
        	if(fstat.getPath().getName().endsWith(".warc.gz")){
        		logger.info("Accepting Path: " + fstat.getPath().toString());
            	inputhPaths.add(fstat.getPath());
        	}
        	else{
        		logger.info("rejecting path: " + fstat.getPath().getName());
        	}
        }

        WarcFileInputFormat.setInputPaths(job,
                (Path[]) inputhPaths.toArray(new Path[inputhPaths.size()]));
		
		job.setMapperClass(FeatureMapper.class);
		job.setReducerClass(FeatureReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	   	    
		FileOutputFormat.setOutputPath(job, FeatureFilesLocation);
//		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("stopWordsListLocation", "/home/manu/data/stop-words/stop-words_english_1_en.txt");
		
		job.set("mapred.map.child.java.opts","-Xmx512m -XX:MaxPermSize=256m");
		job.set("mapred.reduce.child.java.opts","-Xmx512m -XX:MaxPermSize=256m");
		job.setBoolean("mapred.map.tasks.speculative.execution",false);
		job.setBoolean("mapred.reduce.tasks.speculative.execution",false);
		job.setFloat("mapred.reduce.slowstart.completed.maps", (float) 1.0);
		job.setBoolean("mapred.skip.mode.enabled", true);
		job.setInt("mapred.skip.reduce.max.skip.records", 1);
		job.setInt("mapred.skip.attempts.to.start.skipping",1);	
		job.setInt("mapreduce.reduce.input.limit", -1);
		JobClient.runJob(job);
		FileUtil.copyMerge(fs, FeatureFilesLocation, fs, MergedFeatureFilesLocation, true, job, null);
	}
}
