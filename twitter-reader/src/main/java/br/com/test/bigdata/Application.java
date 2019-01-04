package br.com.test.bigdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

import java.text.SimpleDateFormat;

@SpringBootApplication
public class Application implements CommandLineRunner {

    @Autowired
    private Environment env;

    @Autowired
    private Twitter twitter;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public Twitter getTwitterTemplate() {
        String consumerKey = env.getProperty("twitter.consumerKey");
        String consumerSecret = env.getProperty("twitter.consumerSecret");
        String accessToken = env.getProperty("twitter.accessToken");
        String accessTokenSecret = env.getProperty("twitter.accessTokenSecret");

        return new TwitterTemplate(consumerKey, consumerSecret, accessToken, accessTokenSecret);
    }

    @Override
    public void run(String... args) throws Exception {
        for (Tweet tweet : twitter.timelineOperations().getHomeTimeline()) {
            String createdAt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(tweet.getCreatedAt());
            System.out.println(String.format("%s - %s: %s", createdAt, tweet.getFromUser(), tweet.getText()));
        }
    }
}
