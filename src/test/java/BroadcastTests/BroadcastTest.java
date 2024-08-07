package BroadcastTests;


import com.broadcast.functions.BroadcastFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class BroadcastTest
{
	private static final ObjectMapper mapper = new ObjectMapper();

	static List<String> output = new ArrayList<>();

	private static SourceFunction<String> eventFakeSource()
	{
		return new SourceFunction<String>()
		{
			@Override
			public void run(SourceContext<String> ctx) throws Exception
			{
				int count = 0;
				while (count < 1)
				{
					String deserializedString = "";
					String input = "";
					String eventPath = new File("./").getCanonicalPath();
					SimpleStringSchema deserializerSchema = new SimpleStringSchema();

					if (count == 0)
					{
						eventPath += "/src/test/java/events/event.txt";
						input = new String(Files.readAllBytes(Paths.get(eventPath)));
						deserializedString = deserializerSchema.deserialize(input.getBytes());
					}
					ctx.collect(deserializedString);
					Thread.sleep(2000);
					count++;
				}
			}

			@Override
			public void cancel()
			{
			}
		};
	}

	private static SourceFunction<String> ruleFakeSource()
	{
		return new SourceFunction<String>()
		{
			@Override
			public void run(SourceContext<String> ctx) throws Exception
			{
				int count = 0;
				while (count < 1)
				{
					String deserializedString = "";
					String inputRule = "";
					String rulePath = new File("./").getCanonicalPath();
					SimpleStringSchema deserializerSchema = new SimpleStringSchema();

					if (count == 0)
					{
						rulePath += "/src/test/java/events/rule.txt";
						inputRule = new String(Files.readAllBytes(Paths.get(rulePath)));
						deserializedString = deserializerSchema.deserialize(inputRule.getBytes());
					}
					ctx.collect(deserializedString);
					count++;
				}
			}

			@Override
			public void cancel()
			{
			}
		};
	}

	@Test
	void endToEndTest() throws Exception
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SinkFunction<String> fakeSink = new SinkFunction<String>()
		{
			@Override
			public void invoke(String value, Context context) throws Exception
			{
				output.add(value);
			}
		};

		buildFlinkJobCore(env, eventFakeSource(), ruleFakeSource(), fakeSink);
		env.execute();
		String jsonFromKafka = new ObjectMapper().writeValueAsString(output);
		System.out.println("Flink Output: " + jsonFromKafka);
		assertThat(output.size()).isEqualTo(1);
	}

	private void buildFlinkJobCore(StreamExecutionEnvironment env, SourceFunction<String> eventSource,
			SourceFunction<String> ruleSource, SinkFunction<String> sink)
	{

		DataStream<String> sourceStream = env.addSource(eventSource);

		DataStream<String> ruleStream = env.addSource(ruleSource);

		// Broadcast state descriptor
		MapStateDescriptor<String, String> broadcastStateDescriptor = new MapStateDescriptor<>("broadcastState",
				String.class, String.class);
		BroadcastStream<String> broadcastStream = ruleStream.broadcast(broadcastStateDescriptor);

		SingleOutputStreamOperator<String> mainDataStream = sourceStream.connect(broadcastStream)
				.process(new BroadcastFunction(broadcastStateDescriptor)).uid("rule-processor");
		mainDataStream.addSink(sink);
		mainDataStream.print();
	}
}
