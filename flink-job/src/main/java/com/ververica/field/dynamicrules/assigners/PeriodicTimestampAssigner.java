package com.ververica.field.dynamicrules.assigners;

import com.ververica.field.dynamicrules.sources.TimeBasedEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.Serializable;

public class PeriodicTimestampAssigner<T extends TimeBasedEvent>  implements AssignerWithPeriodicWatermarks<T>, Serializable{
  public Long timeSpeedMultiplier;
  public Long timePaddingMs;

  public PeriodicTimestampAssigner(Long timeSpeedMultiplier, Long timePaddingMs) {
    this.timeSpeedMultiplier = timeSpeedMultiplier;
    this.timePaddingMs = timePaddingMs;
  }


  Long firstWatermarkTimeMs = 0L;

  @Nullable
  @Override
  public Watermark getCurrentWatermark() {
    Long currentTimeMs = System.currentTimeMillis();
    if (firstWatermarkTimeMs == 0L) firstWatermarkTimeMs = currentTimeMs;
    Long deltaMs = currentTimeMs - firstWatermarkTimeMs;
    Long watermarkVal = deltaMs * timeSpeedMultiplier - timePaddingMs;
    return new Watermark(watermarkVal);
  }

  @Override
  public long extractTimestamp(T element, long recordTimestamp) {
    return element.getTimestamp() * 1000;
  }


}
