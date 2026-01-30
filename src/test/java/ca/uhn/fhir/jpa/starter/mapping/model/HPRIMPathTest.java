package ca.uhn.fhir.jpa.starter.mapping.model;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HPRIMPathTest {

	@Test
	void constructor_shouldParseSegmentOnly() {
		HPRIMPath path = new HPRIMPath("H");

		assertEquals("H", path.getSegment());
		assertEquals(0, path.getSegmentIndex());
		assertNull(path.getField());
		assertEquals(0, path.getFieldRepetition());
		assertNull(path.getComponent());
		assertNull(path.getSubComponent());
		assertFalse(path.hasExplicitComponent());
	}

	@Test
	void constructor_shouldParseSegmentWithIndex() {
		HPRIMPath path = new HPRIMPath("P[2]");

		assertEquals("P", path.getSegment());
		assertEquals(2, path.getSegmentIndex());
		assertNull(path.getField());
	}

	@Test
	void constructor_shouldParseField() {
		HPRIMPath path = new HPRIMPath("H-7");

		assertEquals("H", path.getSegment());
		assertEquals(0, path.getSegmentIndex());
		assertEquals(6, path.getField()); // 1-based → 0-based
		assertEquals(0, path.getFieldRepetition());
		assertNull(path.getComponent());
		assertFalse(path.hasExplicitComponent());
	}

	@Test
	void constructor_shouldParseFieldWithRepetition() {
		HPRIMPath path = new HPRIMPath("P-8[3]");

		assertEquals("P", path.getSegment());
		assertEquals(7, path.getField());
		assertEquals(3, path.getFieldRepetition());
		assertNull(path.getComponent());
	}

	@Test
	void constructor_shouldParseComponent() {
		HPRIMPath path = new HPRIMPath("P-8-12");

		assertEquals("P", path.getSegment());
		assertEquals(7, path.getField());
		assertEquals(11, path.getComponent()); // 1-based → 0-based
		assertNull(path.getSubComponent());
		assertTrue(path.hasExplicitComponent());
	}

	@Test
	void constructor_shouldParseSubComponent() {
		HPRIMPath path = new HPRIMPath("P-8-12-2");

		assertEquals("P", path.getSegment());
		assertEquals(7, path.getField());
		assertEquals(11, path.getComponent());
		assertEquals(1, path.getSubComponent()); // 1-based → 0-based
		assertTrue(path.hasExplicitComponent());
	}

	@Test
	void constructor_shouldParseFullPathWithSegmentIndexAndRepetition() {
		HPRIMPath path = new HPRIMPath("P[1]-8[2]-12-3");

		assertEquals("P", path.getSegment());
		assertEquals(1, path.getSegmentIndex());
		assertEquals(7, path.getField());
		assertEquals(2, path.getFieldRepetition());
		assertEquals(11, path.getComponent());
		assertEquals(2, path.getSubComponent());
		assertTrue(path.hasExplicitComponent());
	}

	@Test
	void constructor_shouldThrowExceptionForInvalidPath() {
		assertThrows(
			InvalidRequestException.class,
			() -> new HPRIMPath("INVALID-PATH-TEST")
		);
	}

	@Test
	void toString_shouldContainAllParsedElements() {
		HPRIMPath path = new HPRIMPath("P-8-12-2");

		String result = path.toString();

		assertTrue(result.contains("segment='P'"));
		assertTrue(result.contains("field=7"));
		assertTrue(result.contains("component=11"));
		assertTrue(result.contains("subComponent=1"));
	}
}