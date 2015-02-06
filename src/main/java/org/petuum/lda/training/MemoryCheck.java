package org.petuum.lda.training;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;


public class MemoryCheck {
	public static void main(String[] args) throws IOException {
		Iterator<String> lines = FileUtils.lineIterator(new File("/home/manu/repos/lda/tmp/vocab-r-00000"));
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		while(lines.hasNext()){
			String[] splits = lines.next().split("\\t");
			map.put(splits[0]	, Integer.parseInt(splits[1]));
			map.put(splits[0]+"1"	, Integer.parseInt(splits[1]));
		}
		Runtime rt = Runtime.getRuntime();
		System.out.println(((double)rt.maxMemory())/(1024*1024));
		System.out.println(((double)rt.freeMemory())/(1024*1024));
		System.out.println(map.size());
	}
}
