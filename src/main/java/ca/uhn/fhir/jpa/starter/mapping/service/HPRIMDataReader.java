package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.jpa.starter.mapping.model.HPRIMMessage;
import ca.uhn.fhir.jpa.starter.mapping.model.HPRIMSegment;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.parser.ParserConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class HPRIMDataReader {

	private static final Logger logger = LoggerFactory.getLogger(HPRIMDataReader.class);

	/**
	 * Parses a Base64 encoded HPRIM message.
	 */
	public static HPRIMMessage parseData(String content) {
		try {
			String decoded = new String(
				Base64.getDecoder().decode(content),
				StandardCharsets.UTF_8
			);

			return parse(decoded);

		} catch (Exception e) {
			logger.error("Error while parsing HPRIM content", e);
			throw new InternalErrorException("Error while parsing HPRIM content");
		}
	}

	private static HPRIMMessage parse(String raw) {

		HPRIMMessage message = new HPRIMMessage();

		String[] lines = raw.split("\\r?\\n");

		for (String line : lines) {
			if (line.isBlank()) {
				continue;
			}

			String[] parts = line.split("\\|", -1);
			String segmentName = parts[0];

			HPRIMSegment segment = new HPRIMSegment(segmentName);

			for (int i = 1; i < parts.length; i++) {
				String[] components = parts[i].split("\\^", -1);
				segment.addField(components);
			}

			message.addSegment(segment);
		}

		return message;
	}
}
