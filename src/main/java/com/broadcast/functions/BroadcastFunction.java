package com.broadcast.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class BroadcastFunction extends BroadcastProcessFunction<String, String, String>
{
	private static final ObjectMapper mapper = new ObjectMapper();

	private final MapStateDescriptor<String, String> broadcastStateDescriptor;

	public BroadcastFunction(MapStateDescriptor<String, String> broadcastStateDescriptor)
	{
		super();
		this.broadcastStateDescriptor = broadcastStateDescriptor;
	}

	@Override
	public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception
	{
		{
			final String uuid = UUID.randomUUID().toString();
			try
			{
				ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
				String rules = broadcastState.get("rules");
				EventLogger.log(uuid, "Rules", "PROCESS_ELEMENT", rules, uuid, EventLogger.OUTPUTTYPE.NA,
						EventLogger.LOGLEVEL.INFO);

				// search for a rule
				//				Object document = Configuration.defaultConfiguration().jsonProvider().parse(rules);
				//				String ruleName = JsonpathUtil.getValue(document, "$.[?(@.initials=='WS')].initials");

				//business logic
				out.collect(value);
			}
			catch (Exception e)
			{
				EventLogger.log(uuid, "Exception occurred", "PROCESS_ELEMENT", e.toString(), uuid,
						EventLogger.OUTPUTTYPE.NA, EventLogger.LOGLEVEL.INFO);
			}

		}
	}

	@Override
	public void processBroadcastElement(String rule, Context ctx, Collector<String> collector) throws Exception
	{
		final String uuid = UUID.randomUUID().toString();
		ctx.getBroadcastState(broadcastStateDescriptor).put("rules", rule);

		EventLogger.log(uuid, "Rules", "PROCESS_BROADCAST_ELEMENT",
				mapper.writeValueAsString(ctx.getBroadcastState(broadcastStateDescriptor).get("rules")), uuid,
				EventLogger.OUTPUTTYPE.NA, EventLogger.LOGLEVEL.INFO);
	}
}
