package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.util.ArrayList;
import java.util.Map;

public class ReportConsoleThread extends ReportThread {

  public ReportConsoleThread(AgentGroup[] groups, BenchmarkMetric[] metrics, CConfiguration config) {
    this.groupMetrics = metrics;
    this.groups = groups;
    this.reportInterval = config.getInt("report", reportInterval);
  }

  private String time2String(long millis) {
    final long second = 1000;
    final long minute = 60 * second;
    final long hour = 60 * minute;
    final long day = 24 * hour;

    StringBuilder builder = new StringBuilder();
    if (millis > day) {
      long days = millis / day;
      millis = millis % day;
      builder.append(days);
      builder.append('d');
    }
    long hours = millis / hour;
    millis = millis % hour;
    long minutes = millis / minute;
    millis = millis % minute;
    long seconds = millis / second;
    millis = millis % second;

    builder.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    if (millis > 0) builder.append(String.format(".%03d", millis));
    return builder.toString();
  }

  @Override
  public void run() {
    long start = System.currentTimeMillis();
    boolean interrupt = false;
    StringBuilder builder = new StringBuilder();
    ArrayList<Map<String, Long>> previousMetrics =
        new ArrayList<Map<String, Long>>(groups.length);
    for (int i = 0; i < groups.length; i++)
      previousMetrics.add(null);
    long[] previousMillis = new long[groups.length];
    // wake up every minute to report the metrics
    for (int seconds = reportInterval; !interrupt; seconds += reportInterval) {
      long wakeup = start + (seconds * 1000);
      long currentTime = System.currentTimeMillis();
      try {
        if (wakeup > currentTime) Thread.sleep(wakeup - currentTime);
      } catch (InterruptedException e) {
        interrupt = true;
      }
      long millis =
          !interrupt ? seconds * 1000L : System.currentTimeMillis() - start;
      System.out.println(time2String(millis) + " elapsed: ");
      for (int i = 0; i < groups.length; i++) {
        builder.setLength(0);
        builder.append("Group " );
        builder.append(groups[i].getName());
        String sep = ": ";
        int numThreads = groups[i].getNumAgents();
        Map<String, Long> metrics = groupMetrics[i].list();
        Map<String, Long> prev = previousMetrics.get(i);
        for (Map.Entry<String, Long> kv : metrics.entrySet()) {
          String key = kv.getKey();
          long value = kv.getValue();
          builder.append(sep);
          sep = ", ";
          builder.append(String.format(
              "%d %s (%1.1f/sec, %1.1f/sec/thread)",
              value, key,
              value * 1000.0 / millis,
              value * 1000.0 / millis / numThreads
          ));
          if (!interrupt && prev != null) {
            Long previousValue = prev.get(key);
            if (previousValue == null) previousValue = 0L;
            long valueSince = value - previousValue;
            long millisSince = millis - previousMillis[i];
            builder.append(String.format(
                ", %d since last (%1.1f/sec, %1.1f/sec/thread)",
                valueSince,
                valueSince * 1000.0 / millisSince,
                valueSince * 1000.0 / millisSince / numThreads
            ));
          }
        }
        System.out.println(builder.toString());
        previousMetrics.set(i, metrics);
        previousMillis[i] = millis;
      }
    }
  }

}
