package ca.uhn.fhir.jpa.starter.mapping.model;

import ca.uhn.hl7v2.HL7Exception;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Path {

	private static final Pattern PATH_PATTERN = Pattern.compile(
		"^" +
			"((?:[A-Za-z0-9_]+(?:\\[[0-9]+])?\\.)*)" +   // nested group(s), each optionally repeated
			"([A-Z0-9]+)" +                              // segment
			"(?:\\[([0-9]+)])?" +                        // optional segment repetition
			"(?:-" +
			"([1-9][0-9]*)" +                        		// field
			"(?:\\[([0-9]+)])?" +                    		// field repetition
			"(?:-([1-9][0-9]*))?" +                  		// component
			"(?:-([1-9][0-9]*))?" +                  		// subcomponent
			")?$"
	);

	private final List<String> groups = new ArrayList<>();
	private final List<Integer> groupRepetitions = new ArrayList<>();

	private final String segment;
	private final Integer segmentRepetition;

	private final Integer field;
	private final Integer fieldRepetition;
	private final Integer component;
	private final Integer subComponent;

	public Path(String path) throws HL7Exception {
		Matcher matcher = PATH_PATTERN.matcher(path);
		if (!matcher.matches()) {
			throw new HL7Exception("Invalid path: " + path);
		}

		String groupsPart = matcher.group(1);
		if (!groupsPart.isEmpty()) {
			String[] parts = groupsPart.split("\\.");
			for (String part : parts) {
				if (part.isEmpty()) continue;
				if (part.contains("[")) {
					int start = part.indexOf('[');
					int end = part.indexOf(']');
					String name = part.substring(0, start);
					int rep = Integer.parseInt(part.substring(start + 1, end));
					groups.add(name);
					groupRepetitions.add(rep);
				} else {
					groups.add(part);
					groupRepetitions.add(null);
				}
			}
		}

		this.segment = matcher.group(2);
		this.segmentRepetition = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : null;

		this.field = matcher.group(4) != null ? Integer.parseInt(matcher.group(4)) : null;
		this.fieldRepetition = matcher.group(5) != null ? Integer.parseInt(matcher.group(5)) : null;
		this.component = matcher.group(6) != null ? Integer.parseInt(matcher.group(6)) - 1 : null;
		this.subComponent = matcher.group(7) != null ? Integer.parseInt(matcher.group(7)) - 1 : null;
	}

	public List<String> getGroups() { return groups; }
	public List<Integer> getGroupRepetitions() { return groupRepetitions; }

	public String getSegment() { return segment; }
	public Integer getSegmentRepetition() { return segmentRepetition; }

	public Integer getField() { return field; }
	public Integer getFieldRepetition() { return fieldRepetition; }

	public Integer getComponent() { return component; }
	public Integer getSubComponent() { return subComponent; }

	@Override
	public String toString() {
		return "Path{" +
			"groups=" + groups +
			", groupRepetitions=" + groupRepetitions +
			", segment='" + segment + '\'' +
			", segmentRepetition=" + segmentRepetition +
			", field=" + field +
			", fieldRepetition=" + fieldRepetition +
			", component=" + component +
			", subComponent=" + subComponent +
			'}';
	}
}