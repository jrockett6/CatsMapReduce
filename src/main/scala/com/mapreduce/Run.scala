package com.mapreduce

import cats.effect.{ExitCode, IO, IOApp}
import Input.checkArgs

object Run extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    for {
      e <- checkArgs(args).value.map(_ match {
        case Left(s) => println(s)
        case _ => ()
      })
    } yield ExitCode.Success
}

