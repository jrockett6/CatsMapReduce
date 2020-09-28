package com.mapreduce

import cats.implicits._
import cats.effect.{ContextShift, IO, Blocker}
import fs2.{io, text, Stream}
import scala.concurrent.ExecutionContext

import java.io.File
import java.nio.file.Paths
import java.lang.Thread

import external.MapperImpl
import Utils.fileWriterVec


trait UserMapper {
  def emit(key: String): (String, Any)
}

case class Mapper(shardName: String, mapperNum: Int, nReducers: Int) {
  private val basePath = "resources/intermediate"
  private val intFileNames = List.range(0, nReducers).map(n => s"${basePath}/int_${mapperNum}_${n}")
  private val intFileWriters = fileWriterVec(intFileNames.map(new File(_)))

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private def userMapLine: String => String = line => line.split(" ")
    .map(MapperImpl.emit(_))
    .map({ case (key, value) => s"${key} ${value.toString}\n" })
    .mkString("")

  /**
   * Parse file word by word, call the user provided map function,
   * aggregate result and write to intermediate file.
   */
  def map: IO[List[String]] = intFileWriters.use { writerHandles =>
    Stream.resource(Blocker[IO]).flatMap { blocker =>
      io.file.readAll[IO](Paths.get(shardName), blocker, 4096)
        .through(text.utf8Decode)
        .through(text.lines)
        .map(userMapLine)
        .through(text.lines)
        .map(s => (s.hashCode.abs % nReducers, s"${s}\n"))
        .map { case (hash, s) => writerHandles(hash).write(s.getBytes) }
    }.compile.drain >>
      IO(println(s"INFO: Finished mapper ${mapperNum} on thread ID ${Thread.currentThread().getId()}")) >>
        IO(intFileNames)
  }
}


