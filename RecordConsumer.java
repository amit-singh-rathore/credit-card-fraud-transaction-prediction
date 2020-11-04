package cardtransaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;


public class RecordConsumer {
	public static String HOST_IP = null;
	public static String GROUP_ID = null;

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		//Create spark application and streaming context
		SparkConf sparkConf = new SparkConf().setAppName("CCFraudDetection").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		
		//take HOST_IP and Group_ID from user as these might change
		HOST_IP = args[0];
		GROUP_ID = args[1];
		
		//
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "<PUBLIC_IP>:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", GROUP_ID);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);
		
		Collection<String> topics = Arrays.asList("transactions-topic-verified");
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		JavaDStream<String> javadstream = stream.map(x -> x.value());
		
		JavaDStream<CCPoSRecords> javadstreamtransformed = javadstream.map(x -> new CCPoSRecords(x));

		
		javadstreamtransformed.foreachRDD(new VoidFunction<JavaRDD<CCPoSRecords>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<CCPoSRecords> rdd) {
				rdd.foreach(x -> x.ProcessRecords(x));
			}

		});
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			try {
			CCPoSRecords.closeConnection();
			} catch (IOException ex){
				ex.printStackTrace();
			}
			e.printStackTrace();
		}
		jssc.close();

	}

}
