package com.mapreduce

trait Worker

case class Mapper(shardFile: String) extends Worker
