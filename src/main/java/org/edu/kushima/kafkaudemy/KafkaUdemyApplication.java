package org.edu.kushima.kafkaudemy;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaUdemyApplication {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaUdemyApplication.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		SpringApplication.run(KafkaUdemyApplication.class, args);

		String mod = "consumer";

		String topic = "topic2";

		if ("producer".equals(mod)) {

			// Create Producer Properties

			Properties props = new Properties();
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			// Create the producer
			KafkaProducer<String, String> prod = new KafkaProducer<>(props);

			for (int i = 0; i < 10; i++) {
				// create the producer record

				String value = "olá pessoal! [" + i + "]";
				String key = "id_" + i;

				ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

				LOG.info("Key: " + key);
				// send data
				prod.send(record, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception == null) {
							LOG.info("Recebido metadata:");
							LOG.info("Topic: {}", metadata.topic());
							LOG.info("Partition: {}", metadata.partition());
							LOG.info("Offset: {}", metadata.offset());
							LOG.info("Timestamp: {}", metadata.timestamp());
						} else {

						}
					}
				}).get();
			}

			// flush and close producer
			prod.flush();
			prod.close();
		} else {
			String bootstrapServers = "127.0.0.1:9092";
			String groupId = "java_app";

			Properties props = new Properties();
			props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

			consumer.subscribe(Collections.singleton(topic));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				
				for (ConsumerRecord<String, String> record : records) {
					LOG.info("Key: {}. Value: {}", record.key(), record.value());
					LOG.info("Partition: {}, Offset: {}", record.partition(), record.offset());
				}
			}
		}

	}

}
