package com.ververica.field.function.sources

import com.ververica.field.dynamicrules.sources.TimeBasedEvent
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.mutable
import scala.io.Source
import scala.reflect.{ClassTag, _}
import scala.util.{Random, Try}

case class TestSource[T <: TimeBasedEvent : ClassTag](config: TestSourceConfig)
  extends SourceFunction[T] {

  private var source: Source = _

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    source = Source.fromFile(config.inputPath)
    val rawInputLines: Array[String] = source.getLines().toArray
    source.close()
    generateStream(ctx, rawInputLines)
  }

  def generateStream(ctx: SourceFunction.SourceContext[T], rawInputLines: Array[String]): Unit = {
    val rng = new Random(1337)
    val eventQueue: mutable.PriorityQueue[(Long, T)] = mutable.PriorityQueue[(Long, T)]()(Ordering.by(-_._1))
    val eventTuples = rawInputLines.map(rawInput => {
      val event: T = TimeBasedEvent.createFromCsv[T](rawInput, classTag[T].runtimeClass.asInstanceOf[Class[T]])
      val processingTime = event.getTimestamp + rng.nextInt(config.processingTimeDelaySeconds + 1)
      (processingTime, event)
    })
    eventQueue.enqueue(eventTuples: _*)

    val firstEvent = eventQueue.dequeue()
    var currentEvent = firstEvent
    var nextEvent = Try(eventQueue.dequeue()).toOption
    while (nextEvent.isDefined) {
      ctx.collect(currentEvent._2)
      val currentDeltaMs = ((currentEvent._1 - firstEvent._1) * 1000) / config.timeSpeedMultiplier
      val nextDeltaMs = ((nextEvent.get._1 - firstEvent._1) * 1000) / config.timeSpeedMultiplier
      val sleepTimeMs = nextDeltaMs - currentDeltaMs
      Thread.sleep(sleepTimeMs)
      currentEvent = nextEvent.get
      nextEvent = Try(eventQueue.dequeue()).toOption
    }
    // Last item
    ctx.collect(currentEvent._2)
  }

  override def cancel(): Unit = {
    source.close()
  }

}


