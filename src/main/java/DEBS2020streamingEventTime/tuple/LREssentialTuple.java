package DEBS2020streamingEventTime.tuple;

public class LREssentialTuple implements BaseTuple {

  private long timestamp;
  private long vid;
  private int speed;

  public LREssentialTuple(long timestamp, long vid, int speed) {
    this.timestamp = timestamp;
    this.vid = vid;
    this.speed = speed;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  public long getVid() {
    return vid;
  }

  public int getSpeed() {
    return speed;
  }

  @Override
  public String toString() {
    return "LREssentialTuple{" +
        "timestamp=" + timestamp +
        ", vid=" + vid +
        ", speed=" + speed +
        '}';
  }
}
