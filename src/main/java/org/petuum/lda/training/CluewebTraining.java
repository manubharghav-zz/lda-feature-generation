package org.petuum.lda.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class CluewebTraining {
	public static void main(String[] args) throws IOException {
		System.out.println("Starting feature file generation");
		CluewebParser parser = new CluewebParser(new Configuration());
		Path input= new Path(args[0]);
		Path output = new Path(args[1]);
		int vocabsize = Integer.parseInt(args[2]);
		Path vocabPath = new Path(output, "vocab");
		parser.parseData(input, vocabPath);
		
		Path sortedVocabPath = new Path(output, "sorted_vocab");
		Path mergedVocabPath = new Path(output, "merged_sorted_vocab");
		
		WordSort vocabSorter = new WordSort();
		vocabSorter.run(vocabPath, sortedVocabPath, mergedVocabPath);
		
		
//		Path FeatureFilesLocation = new Path(output, "featurefiles");
//		FeatureFileGenerator gen  = new FeatureFileGenerator(new Configuration());
//		gen.generateFeatures(input, FeatureFilesLocation, mergedVocabPath.toString(),vocabsize);
//		System.out.println("Completed Generatignf feature files");
	}
}
