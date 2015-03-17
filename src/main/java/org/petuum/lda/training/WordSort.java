package org.petuum.lda.training;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
        
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

        
public class WordSort {
	private FileSystem fs;
	private static Logger logger = Logger.getLogger(WordSort.class);
	public static class DescendingKeyComparator extends WritableComparator {
	    protected DescendingKeyComparator() {
	        super(DoubleWritable.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	        DoubleWritable key1 = (DoubleWritable) w1;
	        DoubleWritable key2 = (DoubleWritable) w2;          
	        return -1 * key1.compareTo(key2);
	    }
	}
	
	public void run(Path input, Path output, Path mergedOutput) throws IOException{
		Configuration conf = new Configuration();
		JobConf job = new JobConf();
		job.setJobName("Vocabulary Sort");
		job.setOutputKeyClass(Text.class);
		job.setJarByClass(WordSort.class);
		
		logger.info("reading input from: " +input.toString());
		logger.info("Writing sorted Output to : "+output.toString());
		
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(WordSortMapper.class);
		job.setReducerClass(WordSortReducer.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyComparatorClass(DescendingKeyComparator.class);
		TextInputFormat.addInputPath(job,input);
		TextOutputFormat.setOutputPath(job, output);
		job.setInt("mapreduce.reduce.input.limit", -1);
		fs = FileSystem.get(job);
		fs.delete(output, true);
		fs.delete(mergedOutput, true);
		JobClient.runJob(job);
		FileUtil.copyMerge(fs, output, fs, mergedOutput, true, job, null);
		
	}

	public static void main(String[] args) throws Exception {
		WordSort ws = new WordSort();
		ws.run(new Path(args[0]), new Path(args[1]), new Path(args[2]));
	}

}