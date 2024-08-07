package com.broadcast;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.broadcast.functions.BroadcastFunction;
import com.broadcast.serializers.OutputSchemaProducer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Map;
import java.util.Properties;

public class BroadcastJob
{

	public static void main(String[] args) throws Exception
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Map<String, Properties> properties = KinesisAnalyticsRuntime.getApplicationProperties();
		//		String eventSource = properties.get("KafkaEventSource").getProperty("topic");
		//		String sinkTopic = properties.get("KafkaSink").getProperty("topic");

		//docker
		KafkaSource<String> eventSource = KafkaSource.<String> builder().setBootstrapServers("kafka:29092")
				.setTopics("eventSource").setGroupId("event-source-group")
				.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)).build();
		KafkaSource<String> ruleEventSource = KafkaSource.<String> builder().setBootstrapServers("kafka:29092")
				.setTopics("rulesource").setGroupId("rule-group")
				.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)).build();
		Properties sinkProps = new Properties();
		sinkProps.setProperty("bootstrap.servers", "kafka:29092");
		sinkProps.setProperty("transaction.timeout.ms", "90000");
		KafkaSink<String> sink = KafkaSink.<String> builder().setKafkaProducerConfig(sinkProps)
				.setRecordSerializer(new OutputSchemaProducer("sink")).setTransactionalIdPrefix("derived-events")
				.build();

		// source stream
		DataStream<String> sourceStream = env.fromSource(eventSource, WatermarkStrategy.noWatermarks(), "event-source");

		// rules stream
		DataStream<String> ruleStream = env.fromSource(ruleEventSource, WatermarkStrategy.noWatermarks(),
				"Rule source");

		// Broadcast state descriptor
		MapStateDescriptor<String, String> broadcastStateDescriptor = new MapStateDescriptor<>("broadcastState",
				String.class, String.class);

		// Broadcast stream
		BroadcastStream<String> broadcastStream = ruleStream.broadcast(broadcastStateDescriptor);

		SingleOutputStreamOperator<String> outputStream = sourceStream.connect(broadcastStream)
				.process(new BroadcastFunction(broadcastStateDescriptor)).uid("rule-processor");

		outputStream.sinkTo(sink);

		env.execute("Rule processor");
	}

}
