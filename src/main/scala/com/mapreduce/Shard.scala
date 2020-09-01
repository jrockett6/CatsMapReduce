package com.mapreduce

import java.io.{BufferedReader, File, FileOutputStream}

import cats.effect.IO
import cats.implicits._

import Utils.{fileReader, fileWriter}

object Shard {
  private val basePath = "resources/shards/"

  /**
   * Shard the input file to be used by n workers for map and reduce.
   * Copy file line by line to intermediate files
   *
   * TODO: Parse file prematurely to parallelize file copying with cats-effect fibers.
   */
  def shard(inFileName: String, nWorkers: Int): IO[Long] = {
    val inFile = new File(inFileName)
    val shardLength = math.ceil(inFile.length.toFloat / nWorkers.toFloat).toInt

    def writeShard(br: BufferedReader, fos: FileOutputStream, acc: Long, bytesToRead: Long): IO[Long] =
      for {
        line <- IO(br.readLine)
        tot <-
          if (line == null)
            IO.pure(acc)
          else if (acc + line.getBytes.length >= bytesToRead) {
            IO(fos.write(line.getBytes)) >>
            IO.pure(acc + line.getBytes.length)
          } else {
            IO(fos.write(line.getBytes)) >>
            IO(fos.write("\n".getBytes)) >>
            writeShard(br, fos, acc + line.getBytes.length + "\n".getBytes.length, bytesToRead)
          }
      } yield tot

    def split(br: BufferedReader, shardCount: Int, bytesRead: Long, bytesToRead: Long): IO[Long] = {
      if (bytesRead >= bytesToRead) IO(bytesRead)
      else {
        val outFile = new File(basePath + "shard" + shardCount.toString)
        fileWriter(outFile)
          .use(fos => writeShard(br, fos, 0, shardLength)
            >>= (n => split(br, shardCount + 1, bytesRead + n, bytesToRead)))
      }
    }

    fileReader(inFile).use(br => split(br, 0, 0, inFile.length))
  }
}