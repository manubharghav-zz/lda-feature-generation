package org.petuum.lda.training;

import org.petuum.lda.training.*;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;


public class ReadWarcSample {

  public static void main(String[] args) throws IOException {
	  String inputWarcFile="/home/manu/Downloads/0013wb-88.warc.gz";
//    String inputWarcFile="/home/manu/Downloads/ClueWeb09_English_Sample.warc.gz";
    // open our gzip input stream
    GZIPInputStream gzInputStream=new GZIPInputStream(new FileInputStream(inputWarcFile));
    
    // cast to a data input stream
    DataInputStream inStream=new DataInputStream(gzInputStream);
    
    // iterate through our stream
    WarcRecord thisWarcRecord;
    while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
      // see if it's a response record
      if (thisWarcRecord.getHeaderRecordType().equals("response")) {
        // it is - create a WarcHTML record
        WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(thisWarcRecord);
        // get our TREC ID and target URI
        
        String thisTRECID=htmlRecord.getTargetTrecID();
        String thisTargetURI=htmlRecord.getTargetURI();
        // print our data
        System.out.println(thisTRECID + " : " + thisTargetURI);
//        System.out.println(htmlRecord.getRawRecord().getContentUTF8());
        
        System.out.println("--------------------------------------------------------------------------------------------");
      }
    }
    
    inStream.close();
  }
}