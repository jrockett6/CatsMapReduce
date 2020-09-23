package com.mapreduce

import cats.implicits._
import cats.effect.{ContextShift, IO, Resource}
import fs2.Stream

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

import java.util.concurrent.Executors

object Master {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def run(shardFiles: List[String]): IO[Unit] = {
    def getExecutionContext: Resource[IO, ExecutionContext] =
      Resource(IO {
        val executor = Executors.newCachedThreadPool()
        val ec = ExecutionContext.fromExecutor(executor)
        (ec, IO(executor.shutdown()))
      })

    /**
     * Spawns worker in fiber to map over words in file shard using user
     * defined function in the external package. Writes results to
     * intermediate files.
     * Returns list of intermediate files.
     */
    def map(shardFiles: List[String]): IO[List[List[String]]] = getExecutionContext.use { ec =>
      shardFiles
        .zipWithIndex
        .map { case (file, n) => {
          val mapper = Mapper(file, n, shardFiles.length)
          contextShift.evalOn(ec)(mapper.map) } }
        .sequence
      }

    /**
     * Combine results of intermediate files, passing the resulting fs2 stream to reducers
     */
    def shuffle(intFiles: List[List[String]]): List[Stream[IO, String]] =
      intFiles
        .flatten
        .groupBy(_.takeRight(2))
        .map { case (_, intFileNames) => Shuffler(intFileNames).shuffle }
        .toList

    /**
     * Starts a reduce worker in a new fiber.
     */
    def reduce(intStreams: List[Stream[IO, String]]): IO[List[String]] = getExecutionContext.use { ec =>
      intStreams
        .zipWithIndex
        .map { case (stream, n) => {
          val reducer = Reducer(stream, n)
          contextShift.evalOn(ec)(reducer.reduce) } }
        .sequence
    }

    for {
      intFiles <- map(shardFiles.reverse)
      intStreams = shuffle(intFiles)
      resFiles <- reduce(intStreams)
      _ <- IO(println(resFiles))
    } yield resFiles
  }
}