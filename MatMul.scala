import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    
    val mat_m = sc.textFile(args(0)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )	

    val mat_n = sc.textFile(args(1)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )

    val mul = mat_m.map( mat_m => (mat_m._2, mat_m)).join(mat_n.map( mat_n => (mat_n._1,mat_n)))
									 .map{ case (k, (mat_m,mat_n)) => 
										((mat_m._1,mat_n._2),(mat_m._3 * mat_n._3)) }

    val reduceValues = mul.reduceByKey((a,b) => (a+b))

    val sorting = reduceValues.sortByKey(true, 0)

    val res = sorting.map(a => a._1._1 + "," + a._1._2 + "," + a._2)
    res.saveAsTextFile("output")
    
  
    sc.stop()

  }
}

