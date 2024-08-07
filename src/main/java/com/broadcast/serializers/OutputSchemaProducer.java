package com.broadcast.serializers;

import com.broadcast.functions.EventLogger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class OutputSchemaProducer implements KafkaRecordSerializationSchema<String>
{
	private String topic;

	private ObjectMapper mapper;

	public OutputSchemaProducer(String topic)
	{
		super();
		this.topic = topic;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(String obj, KafkaRecordSerializationSchema.KafkaSinkContext kafkaSinkContext, Long timestamp)
	{
		byte[] value = null;
		byte[] key = null;
		if (mapper == null)
		{
			mapper = new ObjectMapper();
		}
		try
		{
			// key needs to be unqiue value which is known so that ordering can occur in Kafka topic
			// currently set for testing
			key = "teststring".getBytes(StandardCharsets.UTF_8);
			value = mapper.writeValueAsBytes(obj);
		}
		catch (Exception e)
		{
			EventLogger
					.log(topic, "Cannot send output to Kafka", "SERIALIZER", "", "", EventLogger.OUTPUTTYPE.ERR,
							EventLogger.LOGLEVEL.ERROR);
		}
		return new ProducerRecord<>(topic, key, value);
	}
}
