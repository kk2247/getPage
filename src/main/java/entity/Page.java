package entity;

/**
 * @author KGZ
 * @date 2019/1/3 19:16
 */
public class Page {
    private String url;

    private String content;

    private String title;

    public Page(String url, String content, String title, int times) {
        this.url = url;
        this.content = content;
        this.title = title;
        this.times = times;
    }

    public Page() {
    }

    private int times;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
    }
}
