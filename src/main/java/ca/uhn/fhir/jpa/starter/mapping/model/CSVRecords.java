package ca.uhn.fhir.jpa.starter.mapping.model;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.util.List;

public class CSVRecords {

    private List<CSVRecord> records;

    public CSVRecords(CSVParser parser) {
        this.records = parser.getRecords();
    }

    public CSVRecords(List<CSVRecord> records) {
        this.records = records;
    }

    public List<CSVRecord> getRecords() {
        return records;
    }
}
