package DEBS2020streamingEventTime.tuple;


public class LRBrakeTuple implements BaseTuple {

  private long timestamp;
  private long vid;
  private float brakeFactor;

  public LRBrakeTuple() {
  }

  public LRBrakeTuple(long timestamp, long vid, float brakeFactor) {
    this.timestamp = timestamp;
    this.vid = vid;
    this.brakeFactor = brakeFactor;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "LRBrakeTuple{" +
        "timestamp=" + timestamp +
        ", vid=" + vid +
        ", brakeFactor=" + brakeFactor +
        '}';
  }
}

