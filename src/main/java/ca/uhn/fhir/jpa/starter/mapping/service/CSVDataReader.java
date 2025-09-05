package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.hl7.fhir.r4.model.DocumentReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * This utility class is used to read CSV Data from different sources.
 */
public class CSVDataReader {

    private static final Logger logger = LoggerFactory.getLogger(CSVDataReader.class);

    private static final String supportedMimeType = "text/csv";

    /**
     * Returns CSV Data from a {@link DocumentReference} resource.
     *
     * @param documentReference the DocumentReference resource.
     * @return the CSV Data using {@link CSVParser}.
     */
    public static CSVParser getCsvData(DocumentReference documentReference) {
        if (documentReference != null && documentReference.hasContent() && !documentReference.getContent().isEmpty()) {
            throw new InvalidRequestException("DocumentReference.content is required !");
        }
        logger.warn("/!\\ Only one content from your DocumentReferenced will be analysed /!\\");
        DocumentReference.DocumentReferenceContentComponent content = documentReference.getContent().get(0);
        if (!content.hasAttachment() && !content.getAttachment().hasData()) {
            throw new InvalidRequestException("DocumentReference.content[0].attachment.data is required for this implementation !");
        } else if (!supportedMimeType.equals(content.getAttachment().getContentType())) {
            throw new InvalidRequestException("DocumentReference.content[0].attachment.contentType must be \"text/csv\" for this implementation !");
        }
        return parseCSVData(content.getAttachment().getDataElement().getValueAsString());
    }

    public static CSVParser parseCSVData(String content) {
        try {
            String csvContent = new String(Base64.getDecoder().decode(content), StandardCharsets.UTF_8);

            CSVFormat format = CSVFormat.EXCEL.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setDelimiter(';')
                    .build();
            return format.parse(new StringReader(csvContent));
        } catch (Exception e) {
            logger.error("Error while reading CSV Data from DocumentReference !", e);
            throw new InternalErrorException("Error while reading CSV Data from DocumentReference !");
        }
    }
}
