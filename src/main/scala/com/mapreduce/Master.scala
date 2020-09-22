package com.mapreduce

import cats.implicits._
import cats.effect.{Async, ContextShift, IO, Resource}

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
    def map(shardFiles: List[String]): IO[List[List[String]]] =
      getExecutionContext.use { ec =>
        shardFiles.zipWithIndex.map(a => {
          val mapper = Mapper(a._1, a._2, shardFiles.length)
          contextShift.evalOn(ec)(mapper.map)
        }).sequence
      }

    /**
     * Combine results of intermediate files, aggregating the values, and
     * calls reduce with the resulting map.
     */
    def combine(intFiles: List[String]): IO[Map[String, Any]] = ???

    /**
     * Starts a reduce worker in a new fiber.
      */
    def reduce(reduceMap: Map[String, Any]): IO[Unit] = ???


    for {
      intFiles <- map(shardFiles.reverse)
      _ <- IO(println(intFiles))
    } yield intFiles
//    for {
//      intFiles <- map(shardFiles)
//      _ <- combine(intFiles)
//          .map(reduce)
//    } yield ()
    }
  }