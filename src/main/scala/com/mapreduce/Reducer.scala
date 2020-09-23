package com.mapreduce

import fs2.{Stream, text}
import cats.effect.IO
import cats.implicits._

import scala.collection.immutable.SortedMap
import java.io.File

import Utils.fileWriter
import external.ReducerImpl

trait UserReducer {
  def emit(k: String, v: List[String]): (String, String)
}

case class Reducer(intStream: Stream[IO, String], reducerNum: Int) {
  private val basePath = "resources/result"
  private val outPath = s"${basePath}/res${reducerNum.toString}"
  private val writer = fileWriter(new File(outPath))

  /**
   * Temporary in-memory solution. Simple solution to read whole stream into sorted map, then write
   * via another stream.
   *
   * TODO: Implement proper external sorting function in shuffle phase to allow for pure stream processing in reducer.
   */
  def reduce: IO[String] = {
    def parseStrings: List[String] => List[(String, String)] =
      ls => ls.flatMap(s => {
        val pair = s.split(" ")
        if (pair.length > 1) List((s.split(" ")(0), s.split(" ")(1)))
        else List[(String, String)]()
      } )

    def aggregateKeys(ls: List[(String, String)]): SortedMap[String, List[String]] =
      ls.foldLeft(SortedMap[String, List[String]]()) { case (acc, pair) =>
        if (acc.contains(pair._1)) acc.updated(pair._1, acc(pair._1).prepended(pair._2))
        else acc.updated(pair._1, List(pair._2))
      }

    def streamToReducedString: IO[String] =
      intStream
      .compile
      .toList
      .map(parseStrings)
      .map(aggregateKeys(_)
        .toList
        .map { case (k, v) => { ReducerImpl.emit(k, v) } }
        .map { case (k, v) => s"${k} ${v}\n" }
        .foldLeft(""){ case (acc, s) => acc ++ s } )

    def writeRes(res: String): IO[Unit] = writer.use { fos =>
      IO(fos.write(res.getBytes))
    }

    for {
      r <- streamToReducedString
      _ <- writeRes(r)
    } yield outPath
  }
}
