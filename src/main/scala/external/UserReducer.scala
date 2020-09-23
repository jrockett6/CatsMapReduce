package external

import com.mapreduce.UserReducer

object ReducerImpl extends UserReducer {
  /**
   * User defined function. Reduce the list of values associated with a key to a single value
   */
  override def emit(k: String, v: List[String]): (String, String) =
    (k, v.map(_.toInt).sum.toString)
}