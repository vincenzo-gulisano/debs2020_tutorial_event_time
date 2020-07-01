package DEBS2020streamingEventTime.tuple;

public class LRInputTuple implements BaseTuple {

  private long timestamp;
  private int type;
  private long vid;
  private int speed;
  private int xway;
  private int lane;
  private int dir;
  private int seg;
  private int pos;

  public static LRInputTuple fromReading(String reading) {
    try {
      String[] tokens = reading.trim().split(",");
      return new LRInputTuple(tokens);
    } catch (Exception exception) {
      throw new IllegalArgumentException(String.format(
          "Failed to parse reading: %s", reading), exception);
    }
  }

  protected LRInputTuple(String[] readings) {
    this(Integer.valueOf(readings[0]),
        Long.valueOf(readings[1]),
        Integer.valueOf(readings[2]),
        Integer.valueOf(readings[3]),
        Integer.valueOf(readings[4]),
        Integer.valueOf(readings[5]),
        Integer.valueOf(readings[6]),
        Integer.valueOf(readings[7]),
        Integer.valueOf(readings[8]),
        System.currentTimeMillis());
  }

  protected LRInputTuple(int type, long time, int vid, int speed,
      int xway, int lane, int dir, int seg, int pos, long stimulus) {
    this.timestamp = time;
    this.type = type;
    this.vid = vid;
    this.speed = speed;
    this.xway = xway;
    this.lane = lane;
    this.dir = dir;
    this.seg = seg;
    this.pos = pos;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long ts) {
    this.timestamp = ts;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public long getVid() {
    return vid;
  }

  public void setVid(long vid) {
    this.vid = vid;
  }

  public int getSpeed() {
    return speed;
  }

  public void setSpeed(int speed) {
    this.speed = speed;
  }

  public int getXway() {
    return xway;
  }

  public void setXway(int xway) {
    this.xway = xway;
  }

  public int getLane() {
    return lane;
  }

  public void setLane(int lane) {
    this.lane = lane;
  }

  public int getDir() {
    return dir;
  }

  public void setDir(int dir) {
    this.dir = dir;
  }

  public int getSeg() {
    return seg;
  }

  public void setSeg(int seg) {
    this.seg = seg;
  }

  public int getPos() {
    return pos;
  }

  public void setPos(int pos) {
    this.pos = pos;
  }

  @Override
  public String toString() {
    return "LRInputTuple{" +
        "timestamp=" + timestamp +
        ", type=" + type +
        ", vid=" + vid +
        ", speed=" + speed +
        ", xway=" + xway +
        ", lane=" + lane +
        ", dir=" + dir +
        ", seg=" + seg +
        ", pos=" + pos +
        '}';
  }
}
