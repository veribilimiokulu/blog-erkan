package com.vbo.datastreamapi

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

// Before run this run data-generator
// (datagen) erkan@ubuntu:~/data-generator$ python dataframe_to_log.py -shf True -b 0.1 -r 5

object ReadFromFile extends App {

  // Create stream execution env
  val env = StreamExecutionEnvironment.getExecutionEnvironment


  val irisDataStreams = env.readFileStream(StreamPath = "file:///home/erkan/data-generator/output",
    intervalMillis=1000L)

  case class Iris(SepalLengthCm: Double,
                  SepalWidthCm: Double,
                  PetalLengthCm: Double,
                  PetalWidthCm: Double,
                  Species: String,
                  ts: String)

  val structuredIris = irisDataStreams.map(line => {
    val SepalLengthCm = line.split(",")(0).toDouble
    val SepalWidthCm = line.split(",")(1).toDouble
    val PetalLengthCm = line.split(",")(2).toDouble
    val PetalWidthCm = line.split(",")(3).toDouble
    val Species = line.split(",")(4)
    val ts = line.split(",")(5)

    Iris(SepalLengthCm, SepalWidthCm, PetalLengthCm, PetalWidthCm, Species, ts)
  })

  structuredIris.map(x => (x.Species, 1))
    .keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(7)))
    .sum(1)
      .print()
  /**
   * 3> (Iris-virginica,22)
   * 4> (Iris-setosa,14)
   * 1> (Iris-versicolor,14)
   * 4> (Iris-setosa,25)
   * 1> (Iris-versicolor,24)
   * 3> (Iris-virginica,21)
   * 3> (Iris-virginica,24)
   * 4> (Iris-setosa,22)
   * 1> (Iris-versicolor,24)
   * 3> (Iris-virginica,22)
   * 4> (Iris-setosa,24)
   * 1> (Iris-versicolor,24)
   * 3> (Iris-virginica,24)
   * 4> (Iris-setosa,23)
   * 1> (Iris-versicolor,23)
   * ...
   * ...
   */

  env.execute("Iris Windowed Count")

}
