package ca.uhn.fhir.jpa.starter.mapping.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HPRIMMessageTest {

	@Test
	@DisplayName("addSegment should add a segment under its name")
	void addSegment_shouldAddSegment() {
		HPRIMMessage message = new HPRIMMessage();
		HPRIMSegment segment = new HPRIMSegment("H");

		message.addSegment(segment);

		List<HPRIMSegment> segments = message.getSegments("H");
		assertEquals(1, segments.size());
		assertSame(segment, segments.get(0));
	}

	@Test
	@DisplayName("addSegment should support multiple segments with the same name")
	void addSegment_shouldSupportMultipleSegmentsWithSameName() {
		HPRIMMessage message = new HPRIMMessage();
		HPRIMSegment segment1 = new HPRIMSegment("P");
		HPRIMSegment segment2 = new HPRIMSegment("P");

		message.addSegment(segment1);
		message.addSegment(segment2);

		List<HPRIMSegment> segments = message.getSegments("P");
		assertEquals(2, segments.size());
		assertSame(segment1, segments.get(0));
		assertSame(segment2, segments.get(1));
	}

	@Test
	@DisplayName("getSegments should return empty list when segment name does not exist")
	void getSegments_shouldReturnEmptyListWhenSegmentDoesNotExist() {
		// given
		HPRIMMessage message = new HPRIMMessage();

		List<HPRIMSegment> segments = message.getSegments("OBX");

		assertNotNull(segments);
		assertTrue(segments.isEmpty());
	}

	@Test
	@DisplayName("getAllSegments should return all segments grouped by name")
	void getAllSegments_shouldReturnAllSegments() {
		HPRIMMessage message = new HPRIMMessage();
		HPRIMSegment h1 = new HPRIMSegment("H");
		HPRIMSegment h2 = new HPRIMSegment("H");
		HPRIMSegment p1 = new HPRIMSegment("P");

		message.addSegment(h1);
		message.addSegment(h2);
		message.addSegment(p1);

		Map<String, List<HPRIMSegment>> allSegments = message.getAllSegments();
		assertEquals(2, allSegments.size());
		assertEquals(2, allSegments.get("H").size());
		assertEquals(1, allSegments.get("P").size());
	}

	@Test
	@DisplayName("getAllSegments should preserve insertion order of segment names")
	void getAllSegments_shouldPreserveInsertionOrder() {
		HPRIMMessage message = new HPRIMMessage();
		message.addSegment(new HPRIMSegment("H"));
		message.addSegment(new HPRIMSegment("P"));
		message.addSegment(new HPRIMSegment("OBR"));

		List<String> keys = message.getAllSegments().keySet().stream().toList();
		assertEquals(List.of("H", "P", "OBR"), keys);
	}
}