package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.jpa.starter.mapping.model.Path;
import ca.uhn.hl7v2.model.GenericSegment;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.model.Group;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MapperTestHL7v2Groups {

	private final String siuS12Message =
		"MSH|^~\\&|TestSystem|TestFacility|ReceiverApp|ReceiverFacility|20251001120000||SIU^S12^SIU_S12|MSG00001|P|2.5.1|||||FRA|UTF-8\r"
			+ "SCH||1234567890^SchedulerApp||||CONSULT^Consultation|||||^^30^202510011230|||||12345^DOE^Alice||||Scheduler|||||Booked\r"
			+ "NTE|||\r"
			+ "PID|||999999999^^^TestAssigningAuthority&1.2.250.1.38.3.1.101&ISO^PI\r"
			+ "PV1||O\r"
			+ "RGS|1\r"
			+ "AIG|1|||DOC001^DOE^Alice\r"
			+ "AIL|1||^||CONSULTROOM";
	private Mapper mapper;

	@BeforeEach
	void setup() {
		mapper = new Mapper(null, null, null, null, null);
	}

	private Object invokePrivate(String methodName, Class<?>[] args, Object... params)
		throws Exception {
		Method method = Mapper.class.getDeclaredMethod(methodName, args);
		method.setAccessible(true);
		return method.invoke(mapper, params);
	}

	@Test
	void shouldFindDirectSegmentAtRootLevel() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Path path = new Path("SCH-2");

		List<GenericSegment> result =
			(List<GenericSegment>) invokePrivate("findSegments",
				new Class[]{Message.class, Path.class}, msg, path);

		assertFalse(result.isEmpty());
		assertEquals("SCH", result.get(0).getName());
	}

	@Test
	void shouldFindSegmentInsideGroup() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Path path = new Path("RESOURCES.GENERAL_RESOURCES.AIG-4");

		List<GenericSegment> result =
			(List<GenericSegment>) invokePrivate("findSegments",
				new Class[]{Message.class, Path.class}, msg, path);

		assertFalse(result.isEmpty());
		assertEquals("AIG", result.get(0).getName());
	}

	@Test
	void shouldReturnExistingSegment() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Path path = new Path("SCH-9999");

		List<GenericSegment> result =
			(List<GenericSegment>) invokePrivate("findSegments",
				new Class[]{Message.class, Path.class}, msg, path);

		assertFalse(result.isEmpty());
		assertEquals("SCH", result.get(0).getName());
	}

	@Test
	void shouldScanGroupsRecursivelyWhenSegmentNotFoundAtExpectedLevel() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Path path = new Path("AIG-4");

		List<GenericSegment> result =
			(List<GenericSegment>) invokePrivate("findSegments",
				new Class[]{Message.class, Path.class}, msg, path);

		assertFalse(result.isEmpty());
		assertEquals("AIG", result.get(0).getName());
	}

	@Test
	void shouldReturnGroupByName() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Group root = (Group) msg;

		List<Group> results =
			(List<Group>) invokePrivate("getGroupsByName",
				new Class[]{Group.class, String.class, Integer.class},
				root, "RESOURCES", null);

		assertFalse(results.isEmpty());
	}

	@Test
	void shouldReturnEmptyListWhenGroupDoesNotExist() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Group root = (Group) msg;

		List<Group> results =
			(List<Group>) invokePrivate("getGroupsByName",
				new Class[]{Group.class, String.class, Integer.class},
				root, "UNKNOWN_GROUP", null);

		assertTrue(results.isEmpty());
	}

	@Test
	void shouldRecursivelyFindSegmentInNestedGroups() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Path path = new Path("AIG-1");

		List<GenericSegment> result =
			(List<GenericSegment>) invokePrivate("scanGroupsRecursively",
				new Class[]{Group.class, Path.class}, msg, path);

		assertFalse(result.isEmpty());
		assertEquals("AIG", result.get(0).getName());
	}

	@Test
	void shouldReturnEmptyWhenSegmentDoesNotExistInAnyGroup() throws Exception {
		Message msg = new PipeParser().parse(siuS12Message);

		Path path = new Path("FOO-1");

		List<GenericSegment> result =
			(List<GenericSegment>) invokePrivate("scanGroupsRecursively",
				new Class[]{Group.class, Path.class}, msg, path);

		assertTrue(result.isEmpty());
	}
}
