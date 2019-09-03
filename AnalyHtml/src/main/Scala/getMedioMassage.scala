import java.io.FileWriter
import java.util.Date
import java.util.ArrayList

import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import scala.collection.mutable.ArrayBuffer

object getMedioMassage extends App {
  val nowtime=new Date().getTime.toString
  val urlpath="http://www.bilibili.com/video/av66255063"
  val outpath="D:\\Users\\avMessage\\%s.txt".format(nowtime)
  val doc = Jsoup.connect(urlpath)
    .data("query", "Java")
    .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36")
    .cookie("auth", "token")
    .ignoreContentType(true)
    .timeout(3000)
    .get()
  val bodyElement=doc.body()
  var MeaasageMap:Map[String,String]=Map()
  val info=bodyElement.getElementsByClass("name").toArray
  val line=info(0).asInstanceOf[Element]
  val userurl=line.getElementsByTag("a").eachAttr("href").toArray
  val username=line.getElementsByTag("a").eachText().toArray

  MeaasageMap+=(username(0).toString->userurl(0).toString)
  MeaasageMap.foreach(println)



}
