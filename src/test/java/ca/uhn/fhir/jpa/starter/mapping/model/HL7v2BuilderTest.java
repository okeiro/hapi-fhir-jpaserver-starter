package ca.uhn.fhir.jpa.starter.mapping.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HL7v2BuilderTest {

	@Test
	void putByPath_shouldCreateSimpleField() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.putByPath("MSH-10", "MSGID");

		String result = builder.build();
		assertEquals("MSH||||||||||MSGID\r", result);
	}

	@Test
	void putByPath_shouldCreateComponent() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.putByPath("PID-5-1", "DOE");
		builder.putByPath("PID-5-2", "JOHN");

		String result = builder.build();
		assertEquals("PID|||||DOE^JOHN\r", result);
	}

	@Test
	void putByPath_shouldCreateSubComponent() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.putByPath("OBX-5-1-1", "ABC");
		builder.putByPath("OBX-5-1-2", "DEF");

		String result = builder.build();
		assertEquals("OBX|||||ABC&DEF\r", result);
	}

	@Test
	void putByPath_shouldAppendRepetitionWithPlusToken() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.putByPath("PID-3[+]-1", "ID1");
		builder.putByPath("PID-3[+]-1", "ID2");

		String result = builder.build();
		assertEquals("PID|||ID1~ID2\r", result);
	}

	@Test
	void putByPath_shouldAppendComponentWithPlusToken() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.putByPath("PID-5-+", "DOE");
		builder.putByPath("PID-5-+", "JOHN");

		String result = builder.build();
		assertEquals("PID|||||DOE^JOHN\r", result);
	}

	@Test
	void addSegment_shouldAddCompleteSegment() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.addSegment(
			"OBR",
			List.of(
				new String[]{"0001"},
				new String[]{"5094108743", "4108743"},
				new String[]{"NF", "NFP", "L"}
			)
		);

		String result = builder.build();
		assertEquals("OBR|0001|5094108743^4108743|NF^NFP^L\r", result);
	}

	@Test
	void putByPath_shouldHandleMultipleSegments() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.putByPath("PID-3-1", "ID1");
		builder.putByPath("PID[+]-3-1", "ID2");

		String result = builder.build();
		assertEquals(
			"PID|||ID1\r" +
				"PID|||ID2\r",
			result
		);
	}

	@Test
	void putByPath_shouldIgnoreValueSuffix() {
		HL7v2Builder builder = new HL7v2Builder();

		builder.putByPath("PID-2.value", "PATIENTID");

		String result = builder.build();
		assertEquals("PID||PATIENTID\r", result);
	}

	@Test
	void putByPath_invalidPath_shouldThrowException() {
		HL7v2Builder builder = new HL7v2Builder();

		IllegalArgumentException ex = assertThrows(
			IllegalArgumentException.class,
			() -> builder.putByPath("INVALIDPATH", "X")
		);

		assertTrue(ex.getMessage().contains("Invalid HL7v2 path"));
	}
}