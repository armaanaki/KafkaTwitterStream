package com.armaanaki.smack;

import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

public class TweetSerializer implements Serializer<Tweet>{

	public void close() {
	}

	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	public byte[] serialize(String arg0, Tweet arg1) {
		byte[] data = SerializationUtils.serialize(arg1);
		return data;
	}
}
