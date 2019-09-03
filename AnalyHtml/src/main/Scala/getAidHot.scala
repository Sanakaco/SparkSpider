object getAidHot {
  val conf=new SparkConf().setMaster("local").setAppName("jsonAnaly")
  val sc=SparkContext.getOrCreate(conf)
  val now = new Date().getTime
  val inputpath="D:\\Users\\bilibili"
  val outputpath="D:\\Users\\bilibiliurl\\%s.txt".format(now.toString)
  val hdfsoutputpath="hdfs://node002:8020/user/spark/%s.txt".format(now.toString)
  val txtfile=sc.textFile(inputpath,4)
  //val file=new FileWriter(outputpath)
  val flagmapjson=txtfile.flatMap(_.split(","))
    .filter(item=>item.contains("arcurl"))
    .map(_.split(":"))
    .filter(item=>item(2).length==40)
    .map(item=>("http://www.bilibili.com/video/"+item(2).substring(29,39)))
    .saveAsTextFile(outputpath)
}
