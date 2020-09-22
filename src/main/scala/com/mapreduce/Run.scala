package com.mapreduce

import cats.effect.{ExitCode, IO, IOApp}
import Input.{ checkArgs, parseArgs}
import Shard.shard

object Run extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    for {
      e <- checkArgs(args).value.map {
        case Left(s) => println(s)
        case _ => ()
      }
      argMap = parseArgs(args)
      shards <- shard(argMap("--f").asInstanceOf[String], argMap("--n").asInstanceOf[Int])
      _ <- Master.run(shards)
    } yield ExitCode.Success
}

