package page; /**
 * @author KGZ
 * @date 2019/1/2 10:30
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class GetPage {

    private List<String> urls;

    public GetPage(){
        urls=new ArrayList<String>();
    }

    /**
     * 通过csdn获取数据信息
     * @return 页面信息
     */
    public PageContent getUrl(){
        Document doc = null;
        try {
            doc = Jsoup.connect("https://blog.csdn.net/").get();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Elements alltitle = doc.select("a");
        for(Element element : alltitle){
            String newUrl = element.select("a").attr("href");
            if(newUrl.length()<5||newUrl==""||newUrl.startsWith("http")==false){
                continue;
            }
            int state = 0;
            for(String str:urls){
                if(str.equals(newUrl)){
                    state =1;
                    break;
                }
            }
            if(state==0){
                urls.add(newUrl);
                PageContent pageContent = null;
                try {
                    pageContent = readPage(newUrl);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return pageContent;
            }
        }
        return null;
    }

    public void init(){
        Document doc = null;
        try{
            doc = Jsoup.connect("https://blog.csdn.net/").get();
        }catch (Exception e){
            try {
                throw new Exception();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        //Document doc = Jsoup.connect("https://blog.csdn.net/").get();
        Elements alltitle = doc.select("a");
        String newUrl = "";
        //urls.add("https://blog.csdn.net/");
        List<PageContent> pageContents=new ArrayList<PageContent>();
        while (pageContents.size()<101){
            for(Element element : alltitle){
                newUrl=element.attributes().get("href");
                //System.out.println(newUrl);
                if(newUrl.length()<5||newUrl==""||newUrl.startsWith("http")==false){
                    continue;
                }
                int state = 0;
                for(String str:urls){
                    if(str.equals(newUrl)){
                        state =1;
                        break;
                    }
                }
                if(state==0){
                    urls.add(newUrl);
                    try{
                        doc = Jsoup.connect(newUrl).get();
                    }catch (Exception e){
                        try {
                            doc = Jsoup.connect("https://blog.csdn.net/").get();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                    }
                    alltitle = doc.select("a");
                    if(alltitle.size()==0){
                        try {
                            doc = Jsoup.connect("https://blog.csdn.net/").get();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        alltitle = doc.select("a");
                    }
                    PageContent pageContent= null;
                    try {
                        pageContent = readPage(newUrl);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    pageContents.add(pageContent);
                    //System.out.println(pageContents.size());
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                }
            }
        }
    }

    /**
     * 读取页面信息
     * @param url
     * @return 页面信息
     * @throws Exception
     */
    public PageContent readPage(String url) throws Exception  {
        Document doc = Jsoup.connect(url).get();
        PageContent pageContent=new PageContent();
        Elements paageContent = doc.select("div");
        Elements pageLink=doc.select("a");
        Elements pageTitle=doc.select("title");
        List<String> link=new ArrayList<String>();
        for (Element data : pageLink) {
            if(data.baseUri().equals("#")==false){
                link.add(data.attributes().get("href"));
            }
        }
        String cont="";
        for(Element data:paageContent){
            cont=cont+data.text().trim();
        }
        pageContent.setLinks(link);
        pageContent.setTitle(pageTitle.text());
        pageContent.setContent(cont);
        pageContent.setUrl(url);
        return pageContent;
    }


}
