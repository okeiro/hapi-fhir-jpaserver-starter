package ca.uhn.fhir.jpa.starter.mapping.model;

import ca.uhn.hl7v2.HL7Exception;
import org.junit.jupiter.api.Test;

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
        assertThrows(HL7Exception.class,  () -> new Path("TartoPom"));
    }

    @Test
    void test11() {
        assertThrows(HL7Exception.class,  () -> new Path("PID-"));
    }

    @Test
    void test12() {
        assertThrows(HL7Exception.class,  () -> new Path("PID-2-"));
    }

    @Test
    void test13() {
        assertThrows(HL7Exception.class,  () -> new Path("PID-2-2-"));
    }
}