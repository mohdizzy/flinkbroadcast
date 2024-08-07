package com.broadcast.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class EventLogger
{
	private EventLogger()
	{
	}

	private static final Logger logger = LoggerFactory.getLogger(EventLogger.class);

	private static final ObjectMapper mapper = new ObjectMapper();

	public enum LOGLEVEL
	{
		INFO, ERROR, DEBUG;
	}

	public enum OUTPUTTYPE
	{
		ERR,NA;
	}

	public static void log(String identifierValue, String message, String context, String payload, String flinkUUID,
			OUTPUTTYPE outputEventType, LOGLEVEL logType)
	{
		try
		{
			Map<String, Object> log = new LinkedHashMap<>();
			List<Object> inputEvents = new ArrayList<>();
			Map<String, Object> inputEvent = new LinkedHashMap<>();
			Map<String, String> outputEvent = new LinkedHashMap<>();
			log.put("Message", message);
			log.put("Context", context);
			log.put("Payload", payload);
			inputEvent.put("type", "SourceId");
			inputEvent.put("IdentifierKey", "ID");
			inputEvent.put("IdentifierValue", identifierValue);
			outputEvent.put("type", outputEventType.toString());
			outputEvent.put("Id", flinkUUID);
			inputEvents.add(inputEvent);
			log.put("InputEvents", inputEvents);
			log.put("OutputEvent", outputEvent);

			switch (logType)
			{
			case INFO:
				logger.info(mapper.writeValueAsString(log));
				break;
			case DEBUG:
				logger.debug(mapper.writeValueAsString(log));
				break;
			case ERROR:
				logger.error(mapper.writeValueAsString(log));
				break;
			}
		}
		catch (Exception e)
		{
			logger.error("Logger exception");
		}
	}
}
