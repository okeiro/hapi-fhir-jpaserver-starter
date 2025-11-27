package ca.uhn.fhir.jpa.starter.mapping.model;

import ca.uhn.hl7v2.HL7Exception;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PathTest {

	@Test
	void test1() throws HL7Exception {
		Path path = new Path("PID[1]-3[5]-2-6");

		assertEquals("PID", path.getSegment());
		assertEquals(1, path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertEquals(5, path.getFieldRepetition());
		assertEquals(1, path.getComponent());
		assertEquals(5, path.getSubComponent());
	}

	@Test
	void test2() throws HL7Exception {
		Path path = new Path("PID[1]-3[5]-2");

		assertEquals("PID", path.getSegment());
		assertEquals(1, path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertEquals(5, path.getFieldRepetition());
		assertEquals(1, path.getComponent());
		assertNull(path.getSubComponent());
	}

	@Test
	void test3() throws HL7Exception {
		Path path = new Path("PID[1]-3[5]");

		assertEquals("PID", path.getSegment());
		assertEquals(1, path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertEquals(5, path.getFieldRepetition());
		assertNull(path.getComponent());
		assertNull(path.getSubComponent());
	}

	@Test
	void test4() throws HL7Exception {
		Path path = new Path("PID[1]-3-2-6");

		assertEquals("PID", path.getSegment());
		assertEquals(1, path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertNull(path.getFieldRepetition());
		assertEquals(1, path.getComponent());
		assertEquals(5, path.getSubComponent());
	}

	@Test
	void test5() throws HL7Exception {
		Path path = new Path("PID-3[5]-2-6");

		assertEquals("PID", path.getSegment());
		assertNull(path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertEquals(5, path.getFieldRepetition());
		assertEquals(1, path.getComponent());
		assertEquals(5, path.getSubComponent());
	}

	@Test
	void test6() throws HL7Exception {
		Path path = new Path("PID[1]-3");

		assertEquals("PID", path.getSegment());
		assertEquals(1, path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertNull(path.getFieldRepetition());
		assertNull(path.getComponent());
		assertNull(path.getSubComponent());
	}

	@Test
	void test7() throws HL7Exception {
		Path path = new Path("PID-3");

		assertEquals("PID", path.getSegment());
		assertNull(path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertNull(path.getFieldRepetition());
		assertNull(path.getComponent());
		assertNull(path.getSubComponent());
	}

	@Test
	void test8() throws HL7Exception {
		Path path = new Path("PID-3[5]");

		assertEquals("PID", path.getSegment());
		assertNull(path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertEquals(5, path.getFieldRepetition());
		assertNull(path.getComponent());
		assertNull(path.getSubComponent());
	}

	@Test
	void test9() throws HL7Exception {
		Path path = new Path("PID-3-2-6");

		assertEquals("PID", path.getSegment());
		assertNull(path.getSegmentRepetition());
		assertEquals(3, path.getField());
		assertNull(path.getFieldRepetition());
		assertEquals(1, path.getComponent());
		assertEquals(5, path.getSubComponent());
	}

	@Test
	void test10() {
		assertThrows(HL7Exception.class, () -> new Path("TartoPom"));
	}

	@Test
	void test11() {
		assertThrows(HL7Exception.class, () -> new Path("PID-"));
	}

	@Test
	void test12() {
		assertThrows(HL7Exception.class, () -> new Path("PID-2-"));
	}

	@Test
	void test13() {
		assertThrows(HL7Exception.class, () -> new Path("PID-2-2-"));
	}

	@Test
	void testGroupSimple() throws HL7Exception {
		Path path = new Path("PATIENT.PID-3");

		assertEquals(List.of("PATIENT"), path.getGroups());

		assertEquals("PID", path.getSegment());
		assertEquals(3, path.getField());
	}

	@Test
	void testGroupWithRepetition() throws HL7Exception {
		Path path = new Path("PATIENT[2].VISIT[1].PV1-7");

		assertEquals(List.of("PATIENT", "VISIT"), path.getGroups());
		assertEquals(List.of(2, 1), path.getGroupRepetitions());

		assertEquals("PV1", path.getSegment());
		assertEquals(7, path.getField());
	}

	@Test
	void testNestedGroups() throws HL7Exception {
		Path path = new Path("RESOURCES.GENERAL_RESOURCES.AIG-4");

		assertEquals(List.of("RESOURCES", "GENERAL_RESOURCES"), path.getGroups());

		assertEquals("AIG", path.getSegment());
		assertEquals(4, path.getField());
	}

	@Test
	void testNestedGroupWithSegmentRepetition() throws HL7Exception {
		Path path = new Path("A.B[3].C.DG1[2]-3");

		assertEquals(List.of("A", "B", "C"), path.getGroups());

		assertEquals("DG1", path.getSegment());
		assertEquals(2, path.getSegmentRepetition());
		assertEquals(3, path.getField());
	}

	@Test
	void testGroupAndFullFieldSpec() throws HL7Exception {
		Path path = new Path("ORBITS.ROUND[4].PID-3[2]-4-7");

		assertEquals(List.of("ORBITS", "ROUND"), path.getGroups());

		assertEquals("PID", path.getSegment());
		assertEquals(3, path.getField());
		assertEquals(2, path.getFieldRepetition());
		assertEquals(3, path.getComponent());
		assertEquals(6, path.getSubComponent());
	}

	@Test
	void testGroupInvalid() {
		assertThrows(HL7Exception.class, () -> new Path("PATIENT..PID-3"));
	}

	@Test
	void testGroupEndingWithDot() {
		assertThrows(HL7Exception.class, () -> new Path("PATIENT."));
	}

	@Test
	void testInvalidGroupRepetition() {
		assertThrows(HL7Exception.class, () -> new Path("PATIENT[x].PID-3"));
	}
}