package com.mapreduce

import java.io.{BufferedReader, File, FileOutputStream, OutputStream, FileReader}

import cats.effect.{IO, Resource}

object Utils {
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