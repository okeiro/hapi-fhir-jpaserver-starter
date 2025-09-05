package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

// TODO See for different HL7v2 version ?
public class HL7v2DataReader {

    private static final Logger logger = LoggerFactory.getLogger(HL7v2DataReader.class);

    /**
     * Parses a Base64 encoded HL7v2 string and returns a {@link Message}.
     *
     * @param content The Base64 encoded HL7v2 string to parse.
     * @return A JSONObject parsed from the decoded JSON content.
     * @throws InternalErrorException If there's an error while parsing or decoding the JSON content.
     */
    public static Message parseData(String content) {
        try (HapiContext context = new DefaultHapiContext()) {
            Parser parser = context.getGenericParser();
            String hl7v2Content = new String(Base64.getDecoder().decode(content), StandardCharsets.UTF_8);
            return parser.parse(hl7v2Content);
        } catch (IOException e) {
            logger.error("Error while creating context for HL7v2 parsing !", e);
            throw new InternalErrorException("Error while creating context for HL7v2 parsing !");
        } catch (HL7Exception e) {
            logger.error("Error while reading HL7v2 Data from DocumentReference !", e);
            throw new InternalErrorException("Error while reading HL7v2 Data from DocumentReference !");
        }
    }
}
