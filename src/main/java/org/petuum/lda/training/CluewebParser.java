package org.petuum.lda.training;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;



public class CluewebParser extends Configured {
	private static Logger logger = Logger.getLogger(CluewebParser.class);
	private FileSystem fs;
	public CluewebParser(Configuration conf){
		super(conf);
	}
	public static void main(String[] args) throws IOException {
		String input = args[0];
		String output=args[1];
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);
		Configuration conf = new Configuration();
		CluewebParser job = new CluewebParser(conf);
		job.parseData(inputPath, outputPath);
	}
	
	
	
	public void parseData(Path input, Path outputPath) throws IOException {
		logger.info("Parsing Data from " + input.toString());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		JobConf job = new JobConf();
		job.setJobName("Clue Web Parse Job");
		fs = FileSystem.get(job);
		fs.delete(outputPath, true);
		job.setJarByClass(CluewebParser.class);
//		String classpath = System.getProperty("java.class.path");
//		String[] classpathEntries = classpath.split(File.pathSeparator);
//		for(String s: classpathEntries){
//			System.out.println(s);
//		}
		
		job.setInputFormat(WarcFileInputFormat.class);
//		WarcFileInputFormat.addInputPath(job, input);
		
		job.setMapperClass(ClueWebMapper.class);
		job.setReducerClass(CluewebReducer.class);
		job.setCombinerClass(CluewebCombiner.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    // adding inputs.
	    List<Path> inputhPaths = new ArrayList<Path>();
        FileSystem fs = FileSystem.get(job);
//        FileStatus[] listStatus = fs.listStatus(input);
        FileStatus[] listStatus = fs.globStatus(new Path(input + "*/*/*/*/*/*.warc.gz"));
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
	    
		FileOutputFormat.setOutputPath(job, outputPath);
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
		
	}
}