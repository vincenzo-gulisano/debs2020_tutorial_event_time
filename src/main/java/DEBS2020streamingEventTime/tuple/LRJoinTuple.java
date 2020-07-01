package DEBS2020streamingEventTime.tuple;


public class LRJoinTuple implements BaseTuple {

  private long timestamp;
  private long vid;
  private int speed;
  private float averageSpeed;

  public LRJoinTuple() {
  }

  public LRJoinTuple(long timestamp, long vid, int speed, float averageSpeed) {
    this.timestamp = timestamp;
    this.vid = vid;
    this.speed = speed;
    this.averageSpeed = averageSpeed;
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

  public float getAverageSpeed() {
    return averageSpeed;
  }


  @Override
  public String toString() {
    return "LRJoinTuple{" +
        "timestamp=" + timestamp +
        ", vid=" + vid +
        ", speed=" + speed +
        ", averageSpeed=" + averageSpeed +
        '}';
  }
}

