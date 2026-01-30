package ca.uhn.fhir.jpa.starter.mapping.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class HPRIMSegmentTest {

	@Test
	void constructor_shouldInitializeNameAndEmptyFields() {
		HPRIMSegment segment = new HPRIMSegment("P");

		assertEquals("P", segment.getName());
		assertNotNull(segment.getFields());
		assertTrue(segment.getFields().isEmpty());
	}

	@Test
	void addField_shouldAddSingleField() {
		HPRIMSegment segment = new HPRIMSegment("H");
		String[] field = new String[] { "A", "B", "C" };

		segment.addField(field);

		List<String[]> fields = segment.getFields();
		assertEquals(1, fields.size());
		assertSame(field, fields.get(0));
	}

	@Test
	void addField_shouldPreserveInsertionOrder() {
		HPRIMSegment segment = new HPRIMSegment("P");
		String[] field1 = new String[] { "1" };
		String[] field2 = new String[] { "2" };

		segment.addField(field1);
		segment.addField(field2);

		List<String[]> fields = segment.getFields();
		assertEquals(2, fields.size());
		assertSame(field1, fields.get(0));
		assertSame(field2, fields.get(1));
	}

	@Test
	void getFields_shouldReturnLiveList() {
		HPRIMSegment segment = new HPRIMSegment("P");

		segment.getFields().add(new String[] { "X" });

		assertEquals(1, segment.getFields().size());
	}
}