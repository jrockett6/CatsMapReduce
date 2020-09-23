package com.mapreduce

import java.nio.file.Paths

import cats.effect.{Blocker, ContextShift, IO}
import fs2.{Stream, io, text}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext

case class Shuffler(intFiles: List[String]) {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def shuffle: Stream[IO, String] = {
    @tailrec
    def go(ls: List[String], s: Stream[IO, String]): Stream[IO, String] = ls match {
      case Nil => s
      case ls => go(ls.tail, s.merge {
        Stream.resource(Blocker[IO]).flatMap { blocker =>
          io.file.readAll[IO](Paths.get(ls.head), blocker, 4096)
            .through(text.utf8Decode)
            .through(text.lines)
        }
      })
    }

    go(intFiles, Stream[IO, String]())
  }
}
