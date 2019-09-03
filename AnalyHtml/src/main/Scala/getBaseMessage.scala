import net.sf.json.{JSONArray, JSONObject}
import org.jsoup.Jsoup

object getBaseMessage extends App {
  val baseUrl="https://api.bilibili.com/x/web-interface/archive/stat?aid="
  val urlpath=baseUrl+"66255063"
  val doc = Jsoup.connect(urlpath)
    .data("query", "Java")
    .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36")
    .cookie("auth", "token")
    .ignoreContentType(true)
    .timeout(3000)
    .get()
  val json=JSONObject.fromObject(doc.body().text())
  val dataArray=json.getJSONObject("data")
  var map:Map[String,String]=Map()
  map+=("aid"->dataArray.get("aid").toString)
  map+=("view"->dataArray.get("view").toString)
  map+=("danmaku"->dataArray.get("danmaku").toString)
  map+=("reply"->dataArray.get("reply").toString)
  map+=("favorite"->dataArray.get("favorite").toString)
  map+=("coin"->dataArray.get("coin").toString)
  map+=("share"->dataArray.get("share").toString)
  map+=("like"->dataArray.get("like").toString)
  println(map)




}
