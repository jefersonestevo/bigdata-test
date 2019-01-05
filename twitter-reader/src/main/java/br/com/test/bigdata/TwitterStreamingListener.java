package br.com.test.bigdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.social.twitter.api.*;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class TwitterStreamingListener implements StreamListener {

    @Autowired
    private Twitter twitter;

    @Autowired
    private TweetProcessor tweetProcessor;

    @Autowired
    private ExecutorService executorService;

    private LinkedBlockingQueue<Tweet> queue;

    @PostConstruct
    public void init() {
        queue = new LinkedBlockingQueue<>();
        tweetProcessor.setQueue(queue);

        executorService.execute(tweetProcessor);

        List<StreamListener> listeners = new ArrayList<>();
        listeners.add(this);

        FilterStreamParameters parameters = new FilterStreamParameters();
        parameters.track("Netflix");

        twitter.streamingOperations().filter(parameters, listeners);
    }

    @Override
    public void onTweet(Tweet tweet) {
        this.queue.offer(tweet);
    }

    @Override
    public void onDelete(StreamDeleteEvent deleteEvent) {

    }

    @Override
    public void onLimit(int numberOfLimitedTweets) {

    }

    @Override
    public void onWarning(StreamWarningEvent warningEvent) {

    }
}
