package DEBS2020streamingEventTime;

import DEBS2020streamingEventTime.tuple.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static DEBS2020streamingEventTime.Config.*;

public class FullQuery {

  private static final Time AVERAGE_SPEED_WS = Time.seconds(150);
  private static final Time AVERAGE_SPEED_WA = Time.seconds(60);

  private static final Time JOIN_PAST = Time.seconds(0);
  private static final Time JOIN_FUTURE = Time.seconds(60);

  private static final Time HARDEST_BRAKING_WS = Time.seconds(300);

  public static final int LOAD_FACTOR = 10;

  public static void main(String[] args) throws Exception {

    /* set the parallelism of the Join operator*/
    int parallelismDegree = Integer.parseInt(args[0]);
    System.out.println("parallelism degree of Join: " + parallelismDegree);
    System.out.println("all other operators at parallelism 1");

    /* configure artificial load */
    int doLoad = args[1].equals("true") ? 1 : 0;

    // ---

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.disableOperatorChaining();

    // --- CREATE THE INPUT STREAM
    DataStream<LRInputTuple> inputStream = env
        .addSource(new LRSource(PATH_TO_LR_SOURCE_DATA))
        .setParallelism(1);

    // --- ASSIGN WATERMARKS
    DataStream<LRInputTuple> inputStreamWM = inputStream.
        assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LRInputTuple>() {
          @Override
          public long extractAscendingTimestamp(LRInputTuple t) {
            return t.getTimestamp() * 1000; // cast timestamp to milliseconds
          }
        })
        .setParallelism(1);

    // --- DISCARD UNUSED FIELDS OF THE INPUT
    DataStream<LREssentialTuple> essentialStream = inputStreamWM.
        map(new MapFunction<LRInputTuple, LREssentialTuple>() {
          @Override
          public LREssentialTuple map(LRInputTuple t) {
            return new LREssentialTuple(t.getTimestamp(), t.getVid(), t.getSpeed());
          }
        })
        .name("DiscardUnusedFieldsMap")
        .setParallelism(1);

    // --- AGGREGATE: PRODUCE AVERAGE SPEED
    essentialStream
        .keyBy(LREssentialTuple::getVid)
        .window(SlidingEventTimeWindows.of(AVERAGE_SPEED_WS, AVERAGE_SPEED_WA))
        .apply(new WindowFunction<LREssentialTuple, LRAverageSpeedTuple, Long, TimeWindow>() {
          @Override
          public void apply(Long vid, TimeWindow timeWindow,
              Iterable<LREssentialTuple> tuplesInWindow,
              Collector<LRAverageSpeedTuple> out) throws InterruptedException {
            int sumOfSpeeds = 0;
            int count = 0;

            for (LREssentialTuple t : tuplesInWindow) {
              sumOfSpeeds += t.getSpeed();
              count += 1;
            }

            float averageSpeed = (float) sumOfSpeeds / count;
            LoadSimulator.createLoad(5, 0.25 * doLoad);

            out.collect(new LRAverageSpeedTuple(timeWindow.maxTimestamp(), vid, averageSpeed));
          }
        })
        .name("AverageSpeedAggregate")
        .setParallelism(1)

        // --- JOIN AGGREGATE SPEED WITH INPUT TUPLES (CURRENT SPEED)
        .keyBy(LRAverageSpeedTuple::getVid)
        .intervalJoin(essentialStream.keyBy(LREssentialTuple::getVid))
        .between(JOIN_PAST, JOIN_FUTURE)
        .process(new ProcessJoinFunction<LRAverageSpeedTuple, LREssentialTuple, LRJoinTuple>() {
                   @Override
                   public void processElement(LRAverageSpeedTuple averageSpeedTuple,
                       LREssentialTuple currentSpeedTuple, Context context,
                       Collector<LRJoinTuple> collector) throws InterruptedException {
                     LoadSimulator.createLoad(10, 0.75 * doLoad);
                     collector.collect(new LRJoinTuple(currentSpeedTuple.getTimestamp(),
                         currentSpeedTuple.getVid(), currentSpeedTuple.getSpeed(),
                         averageSpeedTuple.getAverageSpeed()));
                   }
                 }
        )
        .name("SpeedJoin")
        .setParallelism(parallelismDegree)
        .process(new PrintWithContextTimestamp<>("output/full/joinStream.txt"))

        // --- FORWARD TUPLES WHERE CURRENT SPEED IS VERY DIFFERENT FROM AVERAGE SPEED
        .filter(t -> (t.getAverageSpeed() > 20 && t.getSpeed() < 10))
        .name("StrongBrakeFilter")
        .setParallelism(1)

        // --- FIND VEHICLE THAT BRAKED THE HARDEST
        .windowAll(TumblingEventTimeWindows.of(HARDEST_BRAKING_WS))
        .apply(new AllWindowFunction<LRJoinTuple, LRBrakeTuple, TimeWindow>() {
          @Override
          public void apply(TimeWindow timeWindow, Iterable<LRJoinTuple> tuplesInWindow,
              Collector<LRBrakeTuple> out) throws InterruptedException {

            float maxBrakingFactor = 0.0f;
            LRJoinTuple maxBrakingTuple = new LRJoinTuple();

            for (LRJoinTuple t : tuplesInWindow) {
              float brakingFactor = t.getAverageSpeed() / (t.getSpeed() + 0.001f);
              if (brakingFactor > maxBrakingFactor) {
                maxBrakingFactor = brakingFactor;
                maxBrakingTuple = t;
              }
            }
            LoadSimulator.createLoad(LOAD_FACTOR, doLoad);
            out.collect(new LRBrakeTuple(timeWindow.maxTimestamp(),
                maxBrakingTuple.getVid(), maxBrakingFactor));
          }
        })
        .name("MaxBrakeAggregate")
        .setParallelism(1)
        .process(new PrintWithContextTimestamp<>("output/full/maxBrakeStream.txt"))
        .setParallelism(1);

    env.execute("FullQuery");
  }
}
