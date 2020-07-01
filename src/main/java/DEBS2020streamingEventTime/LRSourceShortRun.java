package DEBS2020streamingEventTime;

import DEBS2020streamingEventTime.tuple.LRInputTuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileReader;

import static DEBS2020streamingEventTime.Config.*;


public class LRSourceShortRun extends RichParallelSourceFunction<LRInputTuple> {

  private volatile boolean enabled;
  private long highestTimestampMS;

  public LRSourceShortRun() {
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    enabled = true;
    highestTimestampMS = 0;
  }


  @Override
  public void run(SourceContext<LRInputTuple> ctx) throws Exception {
    String inputFile = PATH_TO_REPOSITORY + "input/LR_source_data_short.txt";
    try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
      String line = br.readLine();
      while (enabled && line != null) {
        LRInputTuple tuple = LRInputTuple.fromReading(line.trim());
        long timestampMS = tuple.getTimestamp() * 1000;
        highestTimestampMS = Math.max(timestampMS, highestTimestampMS);
        process(tuple, ctx, timestampMS);
        line = br.readLine();
      }
    }
    ctx.emitWatermark(new Watermark(highestTimestampMS + 1));
    while (enabled) {
      // stall execution to avoid emitting concluding watermark
    }
  }

  protected void process(LRInputTuple tuple, SourceContext<LRInputTuple> ctx, long timestampMS) {
    ctx.collectWithTimestamp(tuple, timestampMS);
//    ctx.emitWatermark(new Watermark(timestampMS));
  }

  @Override
  public void cancel() {
    enabled = false;
  }
}

