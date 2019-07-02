package org.edu.kushima.kafkaudemy;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaThreadConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaThreadConsumer.class);

	public void run() {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-java-app";
		String topic = "topic2";

		CountDownLatch latch = new CountDownLatch(1);
		Runnable myConsumer = new ConsumerThread(bootstrapServers, groupId, topic, latch);

		Thread myThread = new Thread(myConsumer);
		myThread.start();

		try {
			latch.await();
		} catch (InterruptedException e) {
			LOG.error("Application get interrupted", e);
		} finally {
			LOG.info("Application is closing");
		}

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOG.info("Shutdown!");
			((ConsumerThread) myConsumer).shutdown();
			try {
				latch.await();
			} catch (Exception e) {
				e.printStackTrace();
			}
			LOG.info(("Application terminated."));
		}));
	}

	public class ConsumerThread implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerThread(String bootstrapServers,
			String groupId, String topic, CountDownLatch latch) {
			this.latch = latch;
			Properties props = new Properties();

			props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
		}

		@Override
		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					
					for (ConsumerRecord<String, String> record : records) {
						LOG.info("Key: {}. Value: {}", record.key(), record.value());
						LOG.info("Partition: {}, Offset: {}", record.partition(), record.offset());
					}
				}	
			} catch (WakeupException e) {
				LOG.info("Received shutdown signal.");
			} finally {
				latch.countDown();
			}
		}

		public void shutdown() {
			consumer.wakeup();
		}

	}
}