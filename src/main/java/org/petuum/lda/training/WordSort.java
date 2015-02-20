package org.petuum.lda.training;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
        
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

        
public class WordSort {
	private FileSystem fs;
	private static Logger logger = Logger.getLogger(WordSort.class);
	public static class IntComparator extends WritableComparator {
		public IntComparator() {
			super(IntWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
			return v1.compareTo(v2) * (-1);
		}
	}
	
	public void run(Path input, Path output) throws IOException{
		Configuration conf = new Configuration();
		JobConf job = new JobConf();
		job.setJobName("Vocabulary Sort");
		job.setOutputKeyClass(Text.class);
		job.setJarByClass(WordSort.class);
		
		logger.info("reading input from: " +input.toString());
		logger.info("Writing sorted Output to : "+output.toString());
		
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(WordSortMapper.class);
		job.setReducerClass(WordSortReducer.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyComparatorClass(IntComparator.class);
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job, output);
		job.setInt("mapreduce.reduce.input.limit", -1);
		fs = FileSystem.get(job);
		fs.delete(output, true);
		JobClient.runJob(job);
	}

	public static void main(String[] args) throws Exception {
		WordSort ws = new WordSort();
		ws.run(new Path(args[0]), new Path(args[1]));
	}

}