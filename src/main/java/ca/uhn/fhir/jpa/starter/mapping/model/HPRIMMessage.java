package ca.uhn.fhir.jpa.starter.mapping.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HPRIMMessage {

	private final Map<String, List<HPRIMSegment>> segments = new LinkedHashMap<>();

	public void addSegment(HPRIMSegment segment) {
		segments
			.computeIfAbsent(segment.getName(), k -> new ArrayList<>())
			.add(segment);
	}

	public List<HPRIMSegment> getSegments(String name) {
		return segments.getOrDefault(name, List.of());
	}

	public Map<String, List<HPRIMSegment>> getAllSegments() {
		return segments;
	}
}
