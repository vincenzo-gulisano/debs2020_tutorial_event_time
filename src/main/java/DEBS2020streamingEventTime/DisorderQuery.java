package DEBS2020streamingEventTime;

import DEBS2020streamingEventTime.tuple.LRAverageSpeedTuple;
import DEBS2020streamingEventTime.tuple.LREssentialTuple;
import DEBS2020streamingEventTime.tuple.LRInputTuple;
import DEBS2020streamingEventTime.tuple.LRJoinTuple;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DisorderQuery {

  private static final Time AVERAGE_SPEED_WS = Time.seconds(150);
  private static final Time AVERAGE_SPEED_WA = Time.seconds(60);

  private static final Time JOIN_PAST = Time.seconds(0);
  private static final Time JOIN_FUTURE = Time.seconds(60);

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.disableOperatorChaining();
    env.setParallelism(1);

    // --- CREATE THE INPUT STREAM
    DataStream<LRInputTuple> inputStream = env
        .addSource(new LRSourceShortRun())
        .process(new PrintWithContextTimestamp<>("output/disorder/inputStream.txt"));

    // --- DISCARD UNUSED FIELDS OF THE INPUT
    DataStream<LREssentialTuple> essentialStream = inputStream.
        map(new MapFunction<LRInputTuple, LREssentialTuple>() {
          @Override
          public LREssentialTuple map(LRInputTuple t) {
            return new LREssentialTuple(t.getTimestamp(), t.getVid(), t.getSpeed());
          }
        })
        .setParallelism(4)
        .process(new PrintWithContextTimestamp<>("output/disorder/afterMapStream.txt"));

    // --- AGGREGATE: PRODUCE AVERAGE SPEED
    essentialStream
        .keyBy(LREssentialTuple::getVid)
        .window(SlidingEventTimeWindows.of(AVERAGE_SPEED_WS, AVERAGE_SPEED_WA))
        .apply(new WindowFunction<LREssentialTuple, LRAverageSpeedTuple, Long, TimeWindow>() {
          @Override
          public void apply(Long vid, TimeWindow timeWindow,
              Iterable<LREssentialTuple> tuplesInWindow,
              Collector<LRAverageSpeedTuple> out) {
            int sumOfSpeeds = 0;
            int count = 0;

            for (LREssentialTuple t : tuplesInWindow) {
              sumOfSpeeds += t.getSpeed();
              count += 1;
            }

            float averageSpeed = (float) sumOfSpeeds / count;

            out.collect(new LRAverageSpeedTuple(timeWindow.maxTimestamp(), vid, averageSpeed));
          }
        })
        .setParallelism(1)
        .process(new PrintWithContextTimestamp<>("output/disorder/afterAverageStream.txt"))

        // --- JOIN AGGREGATE SPEED WITH INPUT TUPLES (CURRENT SPEED)
        .keyBy(LRAverageSpeedTuple::getVid)
        .intervalJoin(essentialStream.keyBy(LREssentialTuple::getVid))
        .between(JOIN_PAST, JOIN_FUTURE)
        .process(new ProcessJoinFunction<LRAverageSpeedTuple, LREssentialTuple, LRJoinTuple>() {
                   @Override
                   public void processElement(LRAverageSpeedTuple averageSpeedTuple,
                       LREssentialTuple currentSpeedTuple, Context context,
                       Collector<LRJoinTuple> collector) {
                     collector.collect(new LRJoinTuple(currentSpeedTuple.getTimestamp(),
                         currentSpeedTuple.getVid(), currentSpeedTuple.getSpeed(),
                         averageSpeedTuple.getAverageSpeed()));
                   }
                 }
        )
        .setParallelism(1)

        // --- FORWARD TUPLES WHERE CURRENT SPEED IS VERY DIFFERENT FROM AVERAGE SPEED
        .filter(t -> (t.getAverageSpeed() > 20 && t.getSpeed() < 10))
        .process(new PrintWithContextTimestamp<>("output/disorder/afterFilterStream.txt"));

    env.execute();
  }
}
