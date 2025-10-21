package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.GenericSegment;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.parser.PipeParser;
import org.hl7.fhir.r4.model.StructureMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MapperTestToGenericSegment {

	private IFhirResourceDao<StructureMap> structureMapDao;
	IGenericClient clientStructureMap = null;
	private final Mapper mapper = new Mapper(null, null, null, structureMapDao, clientStructureMap);

	private final String siuS12Message =
		"MSH|^~\\&|TestSystem|TestFacility|ReceiverApp|ReceiverFacility|20251001120000||SIU^S12^SIU_S12|MSG00001|P|2.5.1|||||FRA|UTF-8\r"
			+ "SCH||1234567890^SchedulerApp||||CONSULT^Consultation|||||^^30^202510011230|||||12345^DOE^Alice||||Scheduler|||||Booked\r"
			+ "NTE|||\r"
			+ "PID|||999999999^^^TestAssigningAuthority&1.2.250.1.38.3.1.101&ISO^PI~111111111111111^^^TestNIR&1.2.250.1.213.1.4.8&ISO^NH||DOE^JOHN^^^^^L||19800101|M|||1 MAIN STREET^^TESTCITY^^75000~^^^^^^APT^^75000||~0600000000^PRN^CP^^^^^^^^^0600000000~^NET^^john.doe@example.com||||||||||TestCountry\r"
			+ "PV1||O\r"
			+ "RGS|1\r"
			+ "AIG|1|||DOC001^DOE^Alice\r"
			+ "AIL|1||^||CONSULTROOM";


	@Test
	void shouldReturnSameGenericSegmentWhenInputIsAlreadyGenericSegment() throws Exception {
		Message message = new PipeParser().parse(siuS12Message);
		Segment msh = (Segment) message.get("MSH");

		GenericSegment gs = new GenericSegment(msh.getParent(), msh.getName());
		gs.parse(msh.encode());

		GenericSegment result = invokeToGenericSegment(gs);

		assertSame(gs, result);
	}

	@Test
	void shouldConvertTypedSegmentToGenericSegment() throws Exception {
		Message message = new PipeParser().parse(siuS12Message);
		Segment sch = (Segment) message.get("SCH");

		GenericSegment result = invokeToGenericSegment(sch);

		assertNotNull(result);
		assertEquals("SCH", result.getName());
		assertEquals("DOE", result.getField(16, 0).encode().split("\\^")[1]);
	}

	@Test
	void shouldThrowExceptionForUnsupportedType() {
		Object notASegment = 12345;

		assertThrows(IllegalArgumentException.class, () -> invokeToGenericSegment(notASegment));
	}

	@Test
	void shouldThrowExceptionIfSegmentEncodingFails() throws HL7Exception {
		Segment failingSegment = mock(Segment.class);
		when(failingSegment.encode()).thenThrow(new HL7Exception("Encoding failed"));
		when(failingSegment.getName()).thenReturn("FAKE");
		when(failingSegment.getParent()).thenReturn(null);

		assertThrows(IllegalArgumentException.class, () -> invokeToGenericSegment(failingSegment));
	}

	private GenericSegment invokeToGenericSegment(Object obj) {
		try {
			var method = Mapper.class.getDeclaredMethod("toGenericSegment", Object.class);
			method.setAccessible(true);
			return (GenericSegment) method.invoke(mapper, obj);
		} catch (Exception e) {
			if (e.getCause() instanceof RuntimeException) throw (RuntimeException) e.getCause();
			if (e.getCause() instanceof IllegalArgumentException) throw (IllegalArgumentException) e.getCause();
			throw new RuntimeException(e);
		}
	}
}
