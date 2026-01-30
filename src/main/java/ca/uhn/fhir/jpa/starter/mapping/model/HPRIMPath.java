package ca.uhn.fhir.jpa.starter.mapping.model;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HPRIMPath {

	// Pattern : SEGMENT[index]-FIELD[rep]-COMP-SUB
	private static final Pattern PATH_PATTERN =
		Pattern.compile("([A-Z0-9]{1,3})(?:\\[(\\d+)])?(?:-(\\d+)(?:\\[(\\d+)])?(?:-(\\d+))?(?:-(\\d+))?)?");

	private final String segment;
	private final Integer segmentIndex;
	private final Integer field;
	private final Integer fieldRepetition;
	private final Integer component;
	private final Integer subComponent;
	private final boolean hasExplicitComponent;

	public HPRIMPath(String path) {
		Matcher matcher = PATH_PATTERN.matcher(path);
		if (!matcher.matches()) {
			throw new InvalidRequestException("Invalid HPRIM path: " + path);
		}

		this.segment = matcher.group(1);
		this.segmentIndex = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
		this.field = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) - 1 : null; // HL7/HPRIM fields are 1-based
		this.fieldRepetition = matcher.group(4) != null ? Integer.parseInt(matcher.group(4)) : 0;
		this.component = matcher.group(5) != null ? Integer.parseInt(matcher.group(5)) - 1 : null; // components 1-based
		this.subComponent = matcher.group(6) != null ? Integer.parseInt(matcher.group(6)) - 1 : null; // subcomponents 1-based
		this.hasExplicitComponent = path.matches(".+-\\d+-\\d+.*");
	}

	public String getSegment() { return segment; }
	public Integer getSegmentIndex() { return segmentIndex; }
	public Integer getField() { return field; }
	public Integer getFieldRepetition() { return fieldRepetition; }
	public Integer getComponent() { return component; }
	public Integer getSubComponent() { return subComponent; }
	public boolean hasExplicitComponent() { return hasExplicitComponent; }

	@Override
	public String toString() {
		return "HPRIMPath{" +
			"segment='" + segment + '\'' +
			", segmentIndex=" + segmentIndex +
			", field=" + field +
			", fieldRepetition=" + fieldRepetition +
			", component=" + component +
			", subComponent=" + subComponent +
			'}';
	}
}