package org.petuum.lda.preprocessing;


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
import org.petuum.lda.training.WarcFileInputFormat;



public class Preprocessor extends Configured {
	private static Logger logger = Logger.getLogger(Preprocessor.class);
	private FileSystem fs;
	public Preprocessor(Configuration conf){
		super(conf);
	}
	public static void main(String[] args) throws IOException {
		String input = args[0];
		String output=args[1];
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);
		Configuration conf = new Configuration();
		Preprocessor job = new Preprocessor(conf);
		job.preProcessData(inputPath, outputPath);
	}
	
	
	
	public void preProcessData(Path input, Path outputPath) throws IOException {
		logger.info("PreProcessing Data from " + input.toString());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		JobConf job = new JobConf();
		job.setJobName("Clue Web Parse Job");
		fs = FileSystem.get(job);
		fs.delete(outputPath, true);
		job.setJarByClass(Preprocessor.class);
		
		job.setInputFormat(WarcFileInputFormat.class);
//		WarcFileInputFormat.addInputPath(job, input);
		
		job.setMapperClass(PreProcessMapper.class);
		job.setReducerClass(PreProcessReducer.class);
//	    job.setCombinerClass(PreProcessReducer.class);
	    // adding inputs.
	    List<Path> inputhPaths = new ArrayList<Path>();
        FileSystem fs = FileSystem.get(job);
        FileStatus[] listStatus = fs.globStatus(input);
		for (FileStatus fstat : listStatus) {
			System.out.println(fstat.getPath());
			if (fstat.getPath().getName().endsWith(".warc.gz")) {
				logger.info("Accepting Path: " + fstat.getPath().toString());
				inputhPaths.add(fstat.getPath());
			} else {
				logger.info("rejecting path: " + fstat.getPath().getName());
			}
		}

        WarcFileInputFormat.setInputPaths(job,
                (Path[]) inputhPaths.toArray(new Path[inputhPaths.size()]));
	    
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.set("mapreduce.map.child.java.opts","-Xmx1024m -XX:MaxPermSize=256m");
		job.set("mapreduce.reduce.child.java.opts","-Xmx1024m -XX:MaxPermSize=256m");
		job.setBoolean("mapreduce.map.tasks.speculative.execution",false);
		job.setBoolean("mapreduce.reduce.tasks.speculative.execution",false);
		job.setFloat("mapreduce.reduce.slowstart.completed.maps", (float) 1.0);
		job.setBoolean("mapreduce.skip.mode.enabled", true);
		job.setInt("mapreduce.skip.reduce.max.skip.records", 1);
		job.setInt("mapreduce.task.skip.start.attempts", 2);
		job.setLong("mapreduce.map.skip.maxrecords", 1000);
		job.setInt("mapreduce.skip.attempts.to.start.skipping",1);	
		job.setInt("mapreduce.reduce.input.limit", -1);
		job.setNumReduceTasks(0);
		job.setInt("mapreduce.task.timeout", 1200000);
		JobClient.runJob(job);
	}
}
