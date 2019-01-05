package br.com.test.bigdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class Application {

    @Autowired
    private Environment env;

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

    @Bean
    public ExecutorService taskExecutor() {
        return Executors.newFixedThreadPool(1);
    }

}
