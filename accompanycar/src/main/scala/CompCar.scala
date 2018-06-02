import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object CompCar {
  val timeInterval = 3 * 60
  val threshold = 2

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accompany Car")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    if (args.size != 1)
      printf("Usage: app [URI]\n")
    val inputFile = "data/test1.csv"
    val textFile = sc.textFile(inputFile, 200).cache()

    val res = textFile.map(line => {
      val Array(car_s, xr_s, ts_s) = line.split(",")
      val (car, xr, ts) = (car_s.toInt, xr_s.toInt, ts_s.toLong)
      val arr = new ArrayBuffer[(Int, Long)](1)
      arr.append((xr, ts))
      (car, arr)
    }).reduceByKey(_ ++ _, numPartitions = 200)

    res.flatMap(tup => {
      val (car, arr) = tup
      val ab = new ArrayBuffer[(Int, ArrayBuffer[(Int, Long)])]
      if(arr.length >= threshold){
        for(i <- 0 to arr.length - 1){
          val ca = new ArrayBuffer[(Int, Long)](1)
          ca.append((car, arr(i)._2))
          ab.append((arr(i)._1,ca))
          //ab.append((arr(i)._1, car, arr(i)._2))
        }
      }
      ab
    }).reduceByKey(_ ++ _,numPartitions = 200)

    res.flatMap(tup => {
      val (xr, arr) = tup
      //arr.sortBy(v => (v._2))
      arr.toList.sortBy(v => v._2)
      val arrs = arr.sortBy(v => (v._2))
      val ret = new ArrayBuffer[(Int, Int)]

      var i = 0;
      var sti = 0;
      var t = false
      while(i < arrs.length){
        var j = i;
        val carList = new Array[ArrayBuffer[Int]](timeInterval * 2)
        for (i <- 0 until timeInterval * 2)
          carList(i) = new ArrayBuffer[Int]
        while((j < arrs.length) && (arrs(j)._2 - arrs(i)._2 < 2 * timeInterval)){
          if(arrs(j)._2 - arrs(i)._2 > timeInterval && !t){
            sti = j
            t=true
          }
          carList((arrs(j)._2.toInt - arrs(i)._2.toInt) % (2 * timeInterval)).append(arrs(j)._1)
          j += 1
        }
        for(x <- 0 to carList.length - 1){
          for(y <- x to carList.length - 1){
            for(xi <- 0 to carList(x).length - 1){
              for(yi <- 0 to carList(y).length - 1){
                if(carList(x)(xi) < carList(y)(yi)) ret.append((carList(x)(xi), carList(y)(yi)))
                else if(carList(x)(xi) > carList(y)(yi)) ret.append((carList(y)(yi), carList(x)(xi)))
              }
            }
          }
        }
        if(t)i = sti
        else i = j
        t = false
      }
      ret
    }).map(word => (word, 1))
      .reduceByKey(_ + _, numPartitions = 200)
      .filter(v => v._2 >= threshold)
      .sortBy(v => (-v._2, v._1._1, v._1._2), numPartitions = 200)
      .collect()
      .foreach(println)
  }
}
