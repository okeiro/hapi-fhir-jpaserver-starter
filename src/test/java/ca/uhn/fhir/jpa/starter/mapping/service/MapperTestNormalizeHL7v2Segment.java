package ca.uhn.fhir.jpa.starter.mapping.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class MapperTestNormalizeHL7v2Segment {

	@Test
	void normalizeSegmentName_shouldPreserveRealDigitSegments() throws Exception {
		Mapper mapper = new Mapper(null, null, null, null, null);

		Method m = Mapper.class.getDeclaredMethod("normalizeSegmentName", String.class);
		m.setAccessible(true);

		assertEquals("PV1", (String) m.invoke(mapper, "PV1"));
		assertEquals("PV2", (String) m.invoke(mapper, "PV2"));
		assertEquals("DG1", (String) m.invoke(mapper, "DG1"));
		assertEquals("IN1", (String) m.invoke(mapper, "IN1"));
		assertEquals("GT1", (String) m.invoke(mapper, "GT1"));

		assertEquals("PV1", (String) m.invoke(mapper, "PV12"));
		assertEquals("PV2", (String) m.invoke(mapper, "PV25"));
		assertEquals("DG1", (String) m.invoke(mapper, "DG13"));
		assertEquals("IN2", (String) m.invoke(mapper, "IN210"));
		assertEquals("GT1", (String) m.invoke(mapper, "GT19"));
	}

	@Test
	void normalizeSegmentName_shouldStripDigitsForIndexed3LetterSegments() throws Exception {
		Mapper mapper = new Mapper(null, null, null, null, null);

		Method m = Mapper.class.getDeclaredMethod("normalizeSegmentName", String.class);
		m.setAccessible(true);

		assertEquals("OBR", (String) m.invoke(mapper, "OBR2"));
		assertEquals("OBX", (String) m.invoke(mapper, "OBX10"));
		assertEquals("NTE", (String) m.invoke(mapper, "NTE3"));
	}
}
