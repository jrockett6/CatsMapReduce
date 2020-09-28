package com.mapreduce

import java.io.{BufferedReader, File, FileOutputStream, OutputStream, FileReader}
import org.apache.commons.io.FileUtils

import cats.effect.{IO, Resource}
import cats.implicits._

object Utils {
  def rmDirs(dirList: List[String] = List("intermediate", "result", "shards")): IO[Unit] = {
    val basePath = "resources/"
    IO(dirList.foreach(dirName => {
      val dir = new File(s"${basePath}${dirName}")
      if (dir.exists() && dir.isDirectory) {
        FileUtils.deleteDirectory(dir)
      }
    }))
  }

  def createDirs(dirList: List[String] = List("intermediate", "result", "shards")): IO[Unit] =
    IO(dirList.foreach(dirName => new File(s"${"resources/"}${dirName}").mkdir()))

  def setupDirs: IO[Unit] = rmDirs() >> createDirs()

  def fileReader(f: File): Resource[IO, BufferedReader] =
    Resource.make {
      IO(new BufferedReader(new FileReader(f)) )
    } { br =>
      IO(br.close()).handleErrorWith(_ => IO.unit)
    }

  def fileWriter(f: File): Resource[IO, FileOutputStream] =
    Resource.make {
      IO(new FileOutputStream(f))
    } { os =>
      IO(os.close()).handleErrorWith(_ => IO.unit)
    }

  def fileWriterVec(files: List[File]): Resource[IO, Vector[OutputStream]] =
    Resource.make {
      IO(files.map(f => new FileOutputStream(f)).toVector)
    }{ fVec =>
      IO(fVec.foreach(fos => fos.close()))
    }

  def fileReaderWriter(inFile: File, outFile: File): Resource[IO, (BufferedReader, FileOutputStream)] =
    for {
      reader <- fileReader(inFile)
      writer <- fileWriter(outFile)
    } yield (reader, writer)
}