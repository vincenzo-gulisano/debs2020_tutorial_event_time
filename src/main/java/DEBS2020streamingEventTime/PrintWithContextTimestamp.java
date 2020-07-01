package DEBS2020streamingEventTime;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.*;

import static DEBS2020streamingEventTime.Config.*;

/**
 * Print the internal timestamp of a stream tuple plus the tuple's toString(), then forward the
 * input as output. If a filename is provided, printing is done to file instead of stdout
 **/
public class PrintWithContextTimestamp<T> extends ProcessFunction<T, T> {

  private String filename = "";

  public PrintWithContextTimestamp(String filename) throws IOException {
    int fileCounter = 0;
    while (true) {
      final String targetFilename = PATH_TO_REPOSITORY + filename.concat(String.valueOf(fileCounter));
      File testFile = new File(targetFilename);
      if (!testFile.exists()) {
        this.filename = targetFilename;
        break;
      }
      fileCounter++;
    }
  }


  @Override
  public void processElement(T tuple, Context context, Collector<T> out) throws Exception {
    if (!filename.isEmpty()) {
      FileWriter fr = new FileWriter(new File(filename), true);
      BufferedWriter br = new BufferedWriter(fr);
      br.write("timestamp: " + context.timestamp() + " -- " + tuple);
      br.newLine();
      br.close();
      fr.close();
    } else {
      System.out.println("timestamp: " + context.timestamp() + " -- " + tuple);
    }
    out.collect(tuple);
  }


}

