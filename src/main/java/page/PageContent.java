package page;

import java.util.List;

/**
 * @author KGZ
 * @date 2018/12/23 8:52
 */
public class PageContent {

    private String url;

    private List<String> links;

    private String title;

    private String Content;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return Content;
    }

    public List<String> getLinks() {
        return links;
    }

    public void setLinks(List<String> links) {
        this.links = links;
    }

    public void setContent(String content) {
        Content = content;
    }
}
