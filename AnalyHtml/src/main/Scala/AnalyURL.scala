import org.jsoup.Jsoup
import java.io
import java.io.{File, FileReader, FileWriter}

import net.sf._
import net.sf.json.JSONObject
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import scala.collection.mutable.ArrayBuffer
object AnalyURL extends App {

  //因为哔哩哔哩的排行数据是动态数据，这里用json格式秦秋数据再由spark进行清洗

  var baseurlpath=("https://s.search.bilibili.com/cate/search?callback=j" +
    "queryCallback_bili_67859390867888716&main_ver=v3&s" +
    "earch_type=video&view_type=hot_rank&order=click&copy_right=-1&cate_id=138&page=%d&" +
    "pagesize=20&jsonp=jsonp&time_from=20190901&time_to=20190930&_=1567486022681")
  var baselocalpath = "D:\\Users\\bilibili\\pagedata_%d.txt"
  var localpath="1"
  var urlpath="1"
  for(i<-1.to(20)){
    urlpath=baseurlpath.format(i)
    localpath=baselocalpath.format(i)
    val dc = Jsoup.connect(urlpath)
      .data("query", "Java")
      .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36")
      .cookie("auth", "token")
      .ignoreContentType(true)
      .timeout(3000)
      .get()
    //val dc = Jsoup.connect(url).get()
    val jsontxt=dc.body().text()
    val file = new FileWriter(localpath)
    for (line <- jsontxt) {
      file.write(line)
    }
  }

}