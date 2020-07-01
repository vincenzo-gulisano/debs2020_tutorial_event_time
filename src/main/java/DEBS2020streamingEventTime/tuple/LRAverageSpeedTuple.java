package DEBS2020streamingEventTime.tuple;

public class LRAverageSpeedTuple implements BaseTuple {

    private long timestamp;
    private long vid;
    private float averageSpeed;

    public LRAverageSpeedTuple(long timestamp, long vid, float averageSpeed) {
        this.timestamp = timestamp;
        this.vid = vid;
        this.averageSpeed = averageSpeed;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getVid() {
        return vid;
    }

    public void setVid(long vid) {
        this.vid = vid;
    }

    public float getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(float averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    @Override
    public String toString() {
        return "LRAverageSpeedTuple{" +
                "timestamp=" + timestamp +
                ", vid=" + vid +
                ", averageSpeed=" + averageSpeed +
                '}';
    }
}
