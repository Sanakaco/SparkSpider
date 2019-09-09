package Jsoup

import java.io.{FileWriter, IOError}
import java.util.{Calendar, Date}

import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.lang.time.FastDateFormat
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.{Jsoup, UncheckedIOException}
import org.jsoup.nodes.Document

object getHtmlDocument {
  var fileSavePath=""
  val JsonArrayObj=new JSONArray
  def Connect(url:String)={
    try{
      val dc = Jsoup.connect(url)
        .data("query", "Java")
        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36")
        .cookie("auth", "token")
        .ignoreContentType(true)
        .timeout(3000)
        .get()
      dc
    }
  }
  def getRanklistUrl(page:Int,pagesize:Int,year:Int,mouthStart:Int,mouthEnd:Int)={
    val start=getMonthStart(year,mouthStart)
    val end=getMonthLast(year,mouthEnd)
    var baiseurl="https://s.search.bilibili.com/" +
      "cate/search?main_ver=v3&search_type=video" +
      "&view_type=hot_rank&order=click&copy_right=-1" +
      s"&cate_id=138&page=$page&pagesize=$pagesize&jsonp=jsonp" +
      s"&time_from=$start&time_to=$end&_=1567568402392"
    baiseurl
  }

  def AnalyzeRankList(jsonfileSavedir:String,page:Int,pagenum:Int):Int={
    try{
      var pagenumfor=0
      val url=getRanklistUrl(page,100,2019,7,9)
      val dirPath=new Path(jsonfileSavedir)
      val filePath=new Path(jsonfileSavedir+page.toString)
      val docRank=Connect(url)
      val hdfsconf=new Configuration()
      hdfsconf.set("fs.defaultFS","hdfs:/mycluster")
      val fs=FileSystem.get(hdfsconf)
      if(!fs.exists(dirPath)){
        fs.mkdirs(dirPath)
      }
      val filetext=docRank.body().text()
      val dfs=fs.create(filePath,true)
      dfs.writeUTF(filetext)
      dfs.close()
      if (page<=pagenum){
        val nextPage=page+1
        AnalyzeRankList(jsonfileSavedir,nextPage,pagenum)
      }else{
        pagenum
      }
    }
  }
  def AnalyzeRankList(jsonfileSavedir:String)={

      try {
        var page = 1
        val blocksize = 4 * 1024
        val url = getRanklistUrl(page, 100, 2019, 7, 9)
        val docRank = Connect(url)
        val jsonObj = JSONObject.fromObject(docRank.body().text())
        val pagenum = jsonObj.get("numPages").asInstanceOf[Int]
        val dirPath = new Path(jsonfileSavedir)
        val filePath = new Path(s"$jsonfileSavedir/bilihtml_$page.json")
        val hdfsconf = new Configuration()
        hdfsconf.set("fs.defaultFS", "hdfs://mycluster")
        val fs = FileSystem.get(hdfsconf)
        if (!fs.exists(dirPath)) {
          fs.mkdirs(dirPath)
        }
        val filetext = docRank.body().text()
        val dfs = fs.create(filePath, true)
        println(s"get no.$page web Data")
        val filetextlen = filetext.length
        for (c <- 0.to(filetextlen, blocksize)) {
          if (c < filetextlen - blocksize)
            dfs.writeBytes(filetext.substring(c, c + blocksize))
          else
            dfs.writeBytes(filetext.substring(c, filetextlen))
        }
        dfs.flush()
        dfs.close()
        println(s"get no.$page web Data end")
        while (page<=pagenum) {

          page += 1
          println(s"get no.$page web Data")
          val nexturl = getRanklistUrl(page, 100, 2019, 8, 9)
          val nextfiletext = Connect(nexturl).body().text()
          val nextfilePath = new Path(s"$jsonfileSavedir/bilihtml_$page.json")
          val nextdfs = fs.create(nextfilePath, true)
          val nextfiletextlen = nextfiletext.length
          for (c <- 0.to(nextfiletextlen, blocksize)) {
            if (c < nextfiletextlen - blocksize)
              nextdfs.writeBytes(nextfiletext.substring(c, c + blocksize))
            else
              nextdfs.writeBytes(nextfiletext.substring(c, nextfiletextlen))
          }
          nextdfs.flush()
          nextdfs.close()
        }
        fs.close()
      }

  }
  def AnalyzeVideoMsg(doc:Document,fileSavaPath:String)={
    val MsgJsonObjText=JSONObject.fromObject(doc.body())
      .get("data")
      .toString
    try{
      val file=new FileWriter(fileSavePath,true)
      file.write(MsgJsonObjText)
      file.close()
    }
  }
  def AnalyzeAuthorFans(doc:Document,fileSavePath:String)={
    val FansJsonObj=JSONObject.fromObject(doc.body())
      .getJSONObject("data")
    val MidFans=FansJsonObj.get("mid").toString+":"+FansJsonObj.get("follower").toString
    try{
      val file=new FileWriter(fileSavePath,true)
      file.write(MidFans)
      file.close()
    }
  }
  def AnalyzeAuthorAllPlay(doc:Document,mid:String,fileSavePath:String)={
    val PlayJsonObj=JSONObject.fromObject(doc.body())
      .getJSONObject("data")
      .getJSONObject("archive")
    val MidFans=mid+":"+PlayJsonObj.get("view").toString
    try{
      val file=new FileWriter(fileSavePath,true)
      file.write(MidFans)
      file.close()
    }
  }
  def AnalyzeAuthorNumVideo(doc:Document,mid:String,fileSavePath:String)={
    val NumJsonObj=JSONObject.fromObject(doc.body())
      .getJSONObject("data")
    val MidFans=mid+":"+NumJsonObj.get("video").toString
    try{
      val file=new FileWriter(fileSavePath,true)
      file.write(MidFans)
      file.close()
    }
  }
  def AnalyzeAuthorNumCharge(doc:Document,mid:String,fileSavePath:String)={
    val ChargeJsonObj=JSONObject.fromObject(doc.body())
      .getJSONObject("data")
    val MidFans=mid+":"+ChargeJsonObj.get("count").toString+":"+ChargeJsonObj.get("tocal_count").toString
    try{
      val file=new FileWriter(fileSavePath,true)
      file.write(MidFans)
      file.close()
    }
  }
  def AnalyzeAuthorVideList(doc:Document,fileSavePath:String)={
    try{
      val file=new FileWriter(fileSavePath,true)
      file.write(doc.body().text())
      file.close()
    }
  }
  def getMonthStart(year:Int,mouth:Int):String= {
    var day: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyyMMdd");
    cal.set(Calendar.YEAR,year)
    cal.set(Calendar.MONTH,mouth-1)
    cal.set(Calendar.DATE,1)
    day = df.format(cal.getTime()) //本月第一天
    day
  }
  def getMonthLast(year:Int,mouth:Int):String= {
    var day: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyyMMdd");
    cal.set(Calendar.YEAR,2019)
    cal.set(Calendar.MONTH,8)
    val last=cal.getActualMaximum(Calendar.DAY_OF_MONTH)
    cal.set(Calendar.DATE,last)
    day = df.format(cal.getTime()) //本月第一天
    day
  }
 def conbinerRanklist(hdfsInpath:String,hdfsOutPath:String)={
   val conf=new SparkConf().setAppName("json").setMaster("local")
   val ss=SparkSession.builder().master("local").config(conf).getOrCreate()
   val jsonrdd=ss.read.json(hdfsInpath).select("result")
   jsonrdd.createOrReplaceTempView("ranklist")
   val res=ss.sql("select explode(result) from ranklist").toDF("result")

   val palyHb=res.select("result.mid","result.author","result.id","result.senddate","result.title","result.tag")
   val hdfsconf = new Configuration()
   hdfsconf.set("fs.defaultFS", "hdfs://mycluster")
   val dirpath=new Path(hdfsOutPath)
   val fs = FileSystem.get(hdfsconf)
   if (!fs.exists(dirpath)) {
     fs.mkdirs(dirpath)
   }
   palyHb.show(10)
   palyHb.rdd.saveAsTextFile(s"$dirpath/rankMsg")

 }
  def main(args: Array[String]): Unit = {

    AnalyzeRankList(args(0))
    conbinerRanklist(args(0),args(1))
  }
}

