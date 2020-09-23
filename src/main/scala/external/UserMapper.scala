package external

import com.mapreduce.UserMapper

object MapperImpl extends UserMapper {
  /**
   * User defined function. Map the key (word) parsed from the file to it's appropriate value
   *
   * @param key: String
   * @return (key, val): (String, Any)
   */
  override def emit(key: String): (String, Any) =
    (key, 1)
}

