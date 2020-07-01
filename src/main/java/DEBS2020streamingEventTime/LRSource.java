package DEBS2020streamingEventTime;

import java.io.BufferedReader;
import java.io.FileReader;

import DEBS2020streamingEventTime.tuple.LRInputTuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class LRSource extends RichParallelSourceFunction<LRInputTuple> {

  private final String inputFile;
  private volatile boolean enabled;

  public LRSource(String inputFile) {
    this.inputFile = inputFile;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    enabled = true;
  }

  @Override
  public void run(SourceContext<LRInputTuple> ctx) throws Exception {
    while (enabled) {
      try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
        String line = br.readLine();
        while (enabled && line != null) {
          LRInputTuple tuple = LRInputTuple.fromReading(line.trim());
          process(tuple, ctx);
          line = br.readLine();
        }
      }
    }
  }

  protected void process(LRInputTuple tuple, SourceContext<LRInputTuple> ctx) {
    ctx.collect(tuple);
  }

  @Override
  public void cancel() {
    enabled = false;
  }
}

