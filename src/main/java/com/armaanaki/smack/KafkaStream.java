package com.armaanaki.smack;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

public class KafkaStream {
	public static void main(String[] args) {
		if (args.length < 5) {
			System.err.println("Usage: KafkaStream <kafka brokers> <topic> <Consumer Key> <Consumer Secret> <Access Token> <Access Token Secret>");
		    System.exit(1);
		}
		
		final String topic = args[1];
		Properties props = new Properties();
		props.put("bootstrap.servers", args[0]);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.armaanaki.smack.TweetSerializer");
		
		final KafkaProducer<String, Tweet> kafkaProducer = new KafkaProducer<String, Tweet>(props);
		
		
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.setOAuthConsumer(args[2], args[3]);
		twitterStream.setOAuthAccessToken(new AccessToken(args[4], args[5]));
		StatusListener listener = new StatusListener() {
			private int count = 0;

			public void onException(Exception e) {
				e.printStackTrace();
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
			} 

			public void onScrubGeo(long arg0, long arg1) {
			}

			public void onStallWarning(StallWarning arg0) {
			}

			public void onStatus(Status status) {
				Tweet currentTweet = new Tweet(status.getCreatedAt(), status.getText());
				System.out.println(currentTweet);
				ProducerRecord<String, Tweet> record = new ProducerRecord<String, Tweet>(topic, Integer.toString(count), currentTweet);
				kafkaProducer.send(record);
				count++;
			}

			public void onTrackLimitationNotice(int arg0) {
			}
		};
		twitterStream.addListener(listener);
		FilterQuery query = new FilterQuery();
		query.language("de,en");
		query.track("a");
		twitterStream.filter(query);
	}
}