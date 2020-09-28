package com.mapreduce

import cats.implicits._
import cats.effect.{ContextShift, IO, Resource}
import fs2.Stream

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

import java.util.concurrent.Executors

object Master {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def run(shardFiles: List[String]): IO[List[String]] = {
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
        .parTraverse { case (file, n) =>
          val mapper = Mapper(file, n, shardFiles.length)
          contextShift.evalOn(ec)(IO(println(s"INFO: Starting mapper ${n} . . .")) >> mapper.map) }
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
        .parTraverse { case (stream, n) => {
          val reducer = Reducer(stream, n)
          contextShift.evalOn(ec)(IO(println(s"INFO: Starting reducer ${n} . . .")) >> reducer.reduce) } }
    }

    for {
      _ <- IO(println("\nINFO: Mapping"))
      intFiles <- map(shardFiles.reverse)
      _ <- IO(println("\nINFO: Shuffling intermediate files . . ."))
      intStreams = shuffle(intFiles)
      _ <- IO(println("\nINFO: Reducing"))
      resFiles <- reduce(intStreams)
    } yield resFiles
  }
}