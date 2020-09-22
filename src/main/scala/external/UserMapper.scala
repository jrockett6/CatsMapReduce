package external

import com.mapreduce.UserMapper

import scala.collection.immutable.Map

object MapperImpl extends UserMapper {
  override def emit(key: String): (String, Any) =
    (key, 1)
}

