package ca.uhn.fhir.jpa.starter.mapping.model;

import java.util.ArrayList;
import java.util.List;

public class HPRIMSegment {

	private final String name;
	private final List<String[]> fields;

	public HPRIMSegment(String name) {
		this.name = name;
		this.fields = new ArrayList<>();
	}

	public String getName() {
		return name;
	}

	public List<String[]> getFields() {
		return fields;
	}

	public void addField(String[] components) {
		fields.add(components);
	}
}