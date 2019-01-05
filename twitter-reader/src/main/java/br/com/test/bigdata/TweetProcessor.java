package br.com.test.bigdata;

import org.springframework.social.twitter.api.Tweet;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class TweetProcessor implements Runnable {

    private LinkedBlockingQueue<Tweet> queue;

    public void setQueue(LinkedBlockingQueue<Tweet> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Tweet tweet = queue.take();

                String createdAt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(tweet.getCreatedAt());
                System.out.println(String.format("%s - %s: %s", createdAt, tweet.getFromUser(), tweet.getText()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
