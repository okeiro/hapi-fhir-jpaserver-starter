package ca.uhn.fhir.jpa.starter.mapping.model;

import ca.uhn.hl7v2.HL7Exception;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Path {

    private static final Pattern PATH_PATTERN = Pattern.compile("^([A-Z0-9]+)(\\[([0-9]+)])?(-(([1-9][0-9]*)(\\[([0-9]+)])?)(-([1-9][0-9]*))?(-([1-9][0-9]*))?)?$");

    private String segment;
    private Integer segmentRepetition;

    private Integer field;
    private Integer fieldRepetition;

    private Integer component;

    private Integer subComponent;

    public Path(String path) throws HL7Exception {
        Matcher matcher = PATH_PATTERN.matcher(path);
        if (matcher.find()) {
            this.segment = matcher.group(1);
            this.segmentRepetition = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : null;
            this.field = matcher.group(6) != null ? Integer.parseInt(matcher.group(6)) : null;
            this.fieldRepetition = matcher.group(8) != null ? Integer.parseInt(matcher.group(8)) : null;
            // Component and SubComponent are 1-indexed but the getter uses 0-indexed parameters.
            this.component = matcher.group(10) != null ? Integer.parseInt(matcher.group(10)) - 1 : null;
            this.subComponent = matcher.group(12) != null ? Integer.parseInt(matcher.group(12)) -1 : null;
        } else {
            throw new HL7Exception(String.format("Invalid path : %s", path));
        }
    }

    public String getSegment() {
        return segment;
    }

    public Integer getSegmentRepetition() {
        return segmentRepetition;
    }

    public Integer getField() {
        return field;
    }

    public Integer getFieldRepetition() {
        return fieldRepetition;
    }

    public Integer getComponent() {
        return component;
    }

    public Integer getSubComponent() {
        return subComponent;
    }
}
