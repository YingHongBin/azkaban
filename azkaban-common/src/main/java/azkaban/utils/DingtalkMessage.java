package azkaban.utils;

public class DingtalkMessage {
    private static final String msgtype = "markdown";
    private String title;
    private String text;

    public void setTitle(String title) {
        this.title = title;
    }

    public void setText(String... texts) {
        StringBuilder textBuilder = new StringBuilder();
        for (final String text : texts) {
            textBuilder.append(text);
        }
        this.text = textBuilder.toString();
    }

    @Override
    public String toString() {
        return "{\"msgtype\":\"" + msgtype + "\",\"markdown\":{\"title\":\"" + title + "\",\"text\":\"" + text + "\"}}";
    }
}
