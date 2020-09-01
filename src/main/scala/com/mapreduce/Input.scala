package com.mapreduce

import cats.implicits._
import cats.effect.IO
import cats.data.EitherT
import java.nio.file.{Paths, Files}

object Input {
  private val flagSet = Set("--f", "--n")
  private val basePath = "resources/input/"

  /**
   * Check whether given command line args are valid.
   * Uses EitherT[IO], short circuits on invalid input and
   * returns string with error message in `Left` position.
   *
   * program usage: [--f Input Filename][--n Num Workers]
   */
  def checkArgs(args: List[String]): EitherT[IO, String, Unit] = {
    def checkArgsEmpty(args: List[String]): Either[String, Unit] = {
      val usage = "usage: [--f Input Filename][--n Num Workers]"
      if (args.isEmpty) Left(usage)
      else Right(())
    }

    def checkContainsRequiredArgs(args: List[String]): Either[String, Unit] = {
      val res = flagSet.foldLeft("")((acc, s) => {
        if (!args.contains(s)) acc + s"Missing required flag $s\n"
        else acc
      })
      if (res.isEmpty) Right(())
      else Left(res)
    }

    def checkIfPair(argsPair: List[String]): Either[String, Unit] = argsPair.length match {
      case 2 => Right(())
      case 0 => Left("Pair length 0...?")
      case 1 => Left("No flag/arg found for \"" + argsPair.head + "\"")
      case _ => Left("Pair length somehow is " + argsPair.length.toString)
    }

    def checkValidFlag(flag: String): Either[String, Unit] = {
      if (flagSet.contains(flag)) Right(()) else Left("Invalid flag 's$flag'")
    }

    def checkFileExists(file: String): IO[Boolean] = {
      IO(Files.exists(Paths.get(file)))
    }

    def checkValidNumWorkers(n: String): Either[String, Unit] =
      try {
        Right(n.toInt)
      } catch {
        case e: Exception => Left("s$n is not a valid number")
      }

    def checkValidPair(argsPair: List[String]): EitherT[IO, String, Unit] = {
      val arg = argsPair.tail.head
      argsPair.head match {
        case "--f" => EitherT(
          checkFileExists(s"$basePath/$arg")
            .map(Either.cond(_, (), s"File '$arg' not found"))
        )
        case "--n" => EitherT.fromEither[IO](checkValidNumWorkers(arg))
      }
    }

    def checkPair(pair: List[String]): EitherT[IO, String, Unit] =
      for {
        e <- EitherT.fromEither[IO](checkIfPair(pair))
        e <- EitherT.fromEither[IO](checkValidFlag(pair.head))
        e <- checkValidPair(pair)
      } yield (e)

    for {
      e <- EitherT.fromEither[IO](checkArgsEmpty(args))
      e <- EitherT.fromEither[IO](checkContainsRequiredArgs(args))
      pair <- EitherT.fromEither[IO](Right(args.sliding(2, 2).toList))
      e <- pair.map(checkPair(_)).head
    } yield (e)
  }

  /**
   * Parse command line args.
   *
   * program usage: [--f Input Filename][--n Num Workers]
   */
  def parseArgs(args: List[String]): Map[String, Any] = {
    args.sliding(2, 2).toList.foldLeft(Map[String, Any]())((map, ls) => ls.head match {
      case "--f" => map.updated(ls.head, basePath + ls.tail.head)
      case "--n" => map.updated(ls.head, ls.tail.head.toInt)
    })
  }
}