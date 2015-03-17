package org.petuum.lda.training;

import java.util.HashMap;

import edu.stanford.nlp.process.Morphology;

public class test {
	public static void main(String[] args) {
		String s = "предоставлен";
		Morphology morphAnalyzer = new Morphology();
		
		String stemmed_word = morphAnalyzer.stem(s);
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		map.put(stemmed_word, 2);
		
		
		System.out.println(map.get(stemmed_word));
	}
}
