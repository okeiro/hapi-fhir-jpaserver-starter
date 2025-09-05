package ca.uhn.fhir.jpa.starter.mapping.model;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CSVBuilder {

    private static final String NEW_LINE = "\n";
    private String separator = ",";
    private List<String> headers = new ArrayList<>();
    private List<List<String>> lines = new ArrayList<>();

    public CSVBuilder() {
        this.lines.add(blankLine());
    }

    public CSVBuilder withDelimiter(String separator) {
        if (StringUtils.isEmpty(separator)) {
            throw new IllegalArgumentException("Delimiter cannot be null nor empty.");
        }
        this.separator = separator;
        return this;
    }

    public CSVBuilder addHeader(String header) {
        this.headers.add(header);
        return this;
    }

    public CSVBuilder addLine() {
        this.lines.add(blankLine());
        return this;
    }

    public CSVBuilder addValue(String header, String value) {
        if (StringUtils.isBlank(header)) {
            return this;
        }

        int indexOf = this.headers.indexOf(header);
        if (indexOf < 0) {
            this.headers.add(header);
            indexOf = this.headers.indexOf(header);
        }

        List<String> line = lines.get(lines.size() - 1);
        //If the index is in the bound it means the header was already existing
        if (indexOf < line.size()) {
            if (!StringUtils.isEmpty(line.get(indexOf))) {
                addLine();
                line = lines.get(lines.size() - 1);
            }
            line.set(indexOf, value);
        } else {
            int bufferSize = indexOf - line.size();
            for (int i = 0; i < bufferSize; i++) {
                line.add("");
            }
            line.add(value);
        }
        return this;
    }

    private List<String> blankLine() {
        String[] blankValues = new String[this.headers.size()];
        Arrays.fill(blankValues, "");
        return new ArrayList<>(List.of(blankValues));
    }

    @Override
    public String toString() {
        final StringBuilder toString = new StringBuilder();
        toString.append(this.headers.stream().collect(Collectors.joining(separator)))
                .append(NEW_LINE);
        lines.forEach(line -> toString.append(line.stream().collect(Collectors.joining(separator)))
                .append(NEW_LINE));

        return toString.toString();
    }
}
