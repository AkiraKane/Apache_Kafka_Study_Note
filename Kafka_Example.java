// Start Zookeeper (make a copy of a new zoo.cgfg file in config folder)
bin/zkServer.sh start

bin/zookeeper-server-start.sh config/zookeeper.properties &

// Start Kafka (if 9092 is in use lsof -i :9092; kill -9 3053)
bin/kafka-server-start.sh config/server.properties

// Create Topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic data

// List all topics 
bin/kafka-topics.sh --list --zookeeper localhost:2181

// jps check both the Zookeeper and kafka daemon is up and running

// Producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic data

// Consumer
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic data --from-beginning

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ProducerTest.java
import java.util.properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerTest{
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", "localhost:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list","localhost:9092");
		ProducerConfig config = new ProducerConfig(props);

		Producer producer = new Producer(config);
		String msg = "Welcome Ming Huang";
		producer.send(new KeyedMessage("demo", msg))

	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Creating a DStream from a Kafka Topic
// hostname:port for Kafka brokers, not Zookeeper
val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")

// List of topics you want to listen for from Kafka
val topics = List("testlogs").toSet

// Create our Kafka stream, which will contain (topic, message) pairs, we tack a map(_._2) at the end in order to only get the messages, which contain individual lines of data
val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

// KafkaExample.scala
import org.apache.spark.SparkConf 
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import org.apache.spark.streaming,kafka._
import kafka.serializer.StringDecoder

/** Working example of listening for log data from Kafka's testLogs topic on port 9092 */
object KafkaExample {
	def main(args: Array[String]){
		// Create the context with a 1 second batch size
		val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))
		setypLogging()
		// Construct a regular expression (regex) to extract fields from raw Apache log lines
		val pattern = apacheLogPattern()
		// hostname:port for Kafka brokers, not Zookeeper
		val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
		// List of topics you want to listen for from Kafka
		val topics = List("testlogs").toSet
		val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
		// Extract the request field from each log line
		val requests = lines.map(x => {val matcher: Matcher=pattern.matcher(x); if(matcher.matches()) matcher.get()})
		// Extract the URL from the request
		val urls = requests.map(x => (val arr = x.toString().split(" "); if(arr.size == 3) arr(1) else "[error]"))
		// Reduce by URL over a 5-minute window sliding every second
		val urlCounts = urls.map(x=(x,1)).reduceByKeyAndWindow(_+_, _-_, Seconds(300), Seconds(1))
		// Sort and print the results
		val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x=> x._2, false))
		sortedResults.print()

		// Kick it off
		ssc.checkpoint("C:/checkpoint/")
		ssc.start()
		ssc.awaitTermination()
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Kafka-sample-programs 0.9.0
// Producer.java
import com.goole.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties; 

public class Producer{
	public static void main(String[] args) throws IOException{
		// set up the producer
		KafkaProducer<String, String> producer;
		try(InputStream props = Resources.getResource("producer.props").openStream()){
			Properties properties = New Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		}

		try{
			for(int i = 0; i< 1000000; i++){
				// send lots of message
				producer.send(new ProducerRecord<String, String>(
					"fast-messages",
					String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));

				// every so often send to a different topic
				if(i % 1000 == 0){
					producer.send(new ProducerRecord<String, String>(
						"fast-messages",
						String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
					producer.send(new ProducerRecord<String, String>(
						"summary-markers",
						String .format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
					producer.flush();
					System.out.println("Sent msg number " + i);
				}
			}
		} catch (Throwable throwable){
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			producer.close();
		}
	}
}

// Consumer.java

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class Consumer{
	public static void main(String[] args) throws IOException{
		// set up housekeeping
		ObjectMapper mapper = new ObjectMapper();
		Histogram stats = new Histogram(1, 1000000, 2);
		Histogram global = new Histogram(1, 1000000, 2);

		// the consumer 
		KafkaConsumer<String, String> consumer; 
		try(InputStream props = Resources.getResource("consumer.props").openStream()){
			Properties properties = new Properties();
			properties.load(props);
			if(properties.getProperty("group.id") == null){
				properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
			}
			consumer = new KafkaConsumer<>(properties);
		}
		consumer.subscribe(Arrays.asList("fast-messages", "summary-markers"));
		int timeouts = 0; 
		// no inspection, infinite loop 
		while(true){
			// read records with a short timeout. If we time out, we don't really care
			ConsumerRecords<String, String> records = consumer.poll(200);
			if(records.count() == 0){
				timeouts++;
			} else{
				System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
				timeouts = 0;
			}

			for(ConsumerRecord<String, String> record: records) {
				switch(record.topic()){
					case "fast-messages":
						// the send time is encoded inside the message
						JsonNode msg = mapper.readTree(record.value());
						switch(msg.get("type").asText()){
							case "test":
								long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
								stats.recordValue(latency);
								global.recordValue(latency);
								break;

							case "marker":
								System.out.printf("%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
												  stats.getTotalCount(),
												  stats.getValuePercentile(0), stats.getValueAtPercentile(100),
												  stats.getMean(), stats.getValueAtPercentile(99));
								System.out.printf("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                                        		  global.getTotalCount(),
                                                  global.getValueAtPercentile(0), global.getValueAtPercentile(100),
                                                  global.getMean(), global.getValueAtPercentile(99));

								stats.reset();
								break;

							default:
								throw new IllegalArgumentException("Illegal message type" + msg.get("type"));

					    }
					    break;
					case "summary-markers":
						break;

					default:
						throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
				}
			}
		}
	}
}


