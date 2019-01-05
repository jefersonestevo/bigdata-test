package br.com.test.bigdata.twitter.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.stereotype.Service;

import java.util.concurrent.LinkedBlockingQueue;

@Service
public class TweetProcessor implements Runnable {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;

    private LinkedBlockingQueue<Tweet> queue;
    private String topicName;

    public void setQueue(LinkedBlockingQueue<Tweet> queue) {
        this.queue = queue;
        this.objectMapper = new ObjectMapper();
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Tweet tweet = queue.take();

                kafkaTemplate.send(this.topicName, this.objectMapper.writeValueAsString(tweet));
            } catch (JsonProcessingException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
