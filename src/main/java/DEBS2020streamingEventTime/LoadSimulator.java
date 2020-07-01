package DEBS2020streamingEventTime;

import java.util.Random;

public class LoadSimulator {

  public static void createLoad(int loadFactor, double probabilityThreshold)
      throws InterruptedException {
    Random r = new Random();
    int sleepTime = (int) Math.round(loadFactor + r.nextGaussian() * loadFactor / 3.0);

    boolean doSleep = Math.random() < probabilityThreshold;

    if (sleepTime > 0 && doSleep) {
      Thread.sleep(sleepTime);
    }
  }

  private LoadSimulator() {
  }

}
