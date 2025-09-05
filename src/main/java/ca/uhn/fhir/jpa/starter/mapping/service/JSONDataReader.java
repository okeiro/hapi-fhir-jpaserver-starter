package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hl7.fhir.r4.model.DocumentReference;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class JSONDataReader {
    private static final Logger logger = LoggerFactory.getLogger(JSONDataReader.class);

    private static final String supportedMimeType = "application/json";

    /**
     * Returns JSON Data from a {@link DocumentReference} resource.
     *
     * @param documentReference the DocumentReference resource.
     * @return the JSON Data using {@link ObjectMapper}.
     */
    public static JSONObject getJsonData(DocumentReference documentReference) {
        if (documentReference != null && documentReference.hasContent() && !documentReference.getContent().isEmpty()) {
            throw new InvalidRequestException("DocumentReference.content is required !");
        }
        logger.warn("/!\\ Only one content from your DocumentReferenced will be analysed /!\\");
        DocumentReference.DocumentReferenceContentComponent content = documentReference.getContent().get(0);
        if (!content.hasAttachment() && !content.getAttachment().hasData()) {
            throw new InvalidRequestException("DocumentReference.content[0].attachment.data is required for this implementation !");
        } else if (!supportedMimeType.equals(content.getAttachment().getContentType())) {
            throw new InvalidRequestException("DocumentReference.content[0].attachment.contentType must be \"application/json\" for this implementation !");
        }
        return parseJsonData(content.getAttachment().getDataElement().getValueAsString());
    }

    /**
     * Parses a Base64 encoded JSON string and returns a JSONObject.
     *
     * @param content The Base64 encoded JSON string to parse.
     * @return A JSONObject parsed from the decoded JSON content.
     * @throws InternalErrorException If there's an error while parsing or decoding the JSON content.
     */
    public static JSONObject parseJsonData(String content) {
        try {
            String jsonContent = new String(Base64.getDecoder().decode(content), StandardCharsets.UTF_8);
            return new JSONObject(jsonContent);
        } catch (Exception e) {
            logger.error("Error while reading JSON Data from DocumentReference !", e);
            throw new InternalErrorException("Error while reading JSON Data from DocumentReference !");
        }
    }
}
