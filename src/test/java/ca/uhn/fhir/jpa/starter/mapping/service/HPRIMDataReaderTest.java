package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.jpa.starter.mapping.model.HPRIMMessage;
import ca.uhn.fhir.jpa.starter.mapping.model.HPRIMSegment;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class HPRIMDataReaderTest {

	@Test
	void parseData_shouldParseRealisticHprimMessage_withSyntheticData() {
		String raw =
			"H|^~\\&|||LABSYS^LAB|||ORU|||SITE01^HOSPITAL||P|H2.1^C|20260128121030\n" +
				"P|0001|99999999||ABC1234567890|SMITH^ALEX^^^MR|SMITH|19850517|F||10 MAIN ST^^TESTCITY^^00000||0600000000\n" +
				"OBR|0001|^ORDER123|ACC123^ORDER123|CRP^C Reactive Protein^L|R|202601281205|202601281130||||N|||202601281205||UNIT^SERVICE^L^0001^Ward^L\n" +
				"OBX|1|ST|CRP^C Reactive Protein^L||< 3.0|mg/L|0.0 - 3.0|N|||F|\n" +
				"OBX|2|NM|NA^^L||141|mmol/L|136 - 145|N|||F\n" +
				"L|1||1|31\n";

		String b64 = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
		HPRIMMessage msg = HPRIMDataReader.parseData(b64);

		assertNotNull(msg);

		assertEquals(1, msg.getSegments("H").size());
		assertEquals(1, msg.getSegments("P").size());
		assertEquals(1, msg.getSegments("OBR").size());
		assertEquals(2, msg.getSegments("OBX").size());
		assertEquals(1, msg.getSegments("L").size());

		HPRIMSegment h = msg.getSegments("H").get(0);

		assertArrayEquals(new String[]{"", "~\\&"}, h.getFields().get(0));

		assertArrayEquals(new String[]{""}, h.getFields().get(1));
		assertArrayEquals(new String[]{""}, h.getFields().get(2));

		assertArrayEquals(new String[]{"LABSYS", "LAB"}, h.getFields().get(3));

		assertArrayEquals(new String[]{"H2.1", "C"}, h.getFields().get(12));

		HPRIMSegment p = msg.getSegments("P").get(0);

		assertArrayEquals(new String[]{"0001"}, p.getFields().get(0));
		assertArrayEquals(new String[]{"99999999"}, p.getFields().get(1));
		assertArrayEquals(new String[]{""}, p.getFields().get(2));
		assertArrayEquals(new String[]{"ABC1234567890"}, p.getFields().get(3));

		assertArrayEquals(new String[]{"SMITH", "ALEX", "", "", "MR"}, p.getFields().get(4));

		HPRIMSegment obr = msg.getSegments("OBR").get(0);

		assertArrayEquals(new String[]{"0001"}, obr.getFields().get(0));

		assertArrayEquals(new String[]{"", "ORDER123"}, obr.getFields().get(1));

		assertArrayEquals(new String[]{"CRP", "C Reactive Protein", "L"}, obr.getFields().get(3));

		HPRIMSegment obx1 = msg.getSegments("OBX").get(0);

		assertArrayEquals(new String[]{"1"}, obx1.getFields().get(0));
		assertArrayEquals(new String[]{"ST"}, obx1.getFields().get(1));

		assertArrayEquals(new String[]{"CRP", "C Reactive Protein", "L"}, obx1.getFields().get(2));

		assertArrayEquals(new String[]{""}, obx1.getFields().get(3));

		assertArrayEquals(new String[]{"< 3.0"}, obx1.getFields().get(4));
		assertArrayEquals(new String[]{"mg/L"}, obx1.getFields().get(5));

		assertArrayEquals(new String[]{""}, obx1.getFields().get(obx1.getFields().size() - 1));

		HPRIMSegment obx2 = msg.getSegments("OBX").get(1);

		assertArrayEquals(new String[]{"NA", "", "L"}, obx2.getFields().get(2));

		HPRIMSegment l = msg.getSegments("L").get(0);

		assertArrayEquals(new String[]{"1"}, l.getFields().get(0));
		assertArrayEquals(new String[]{""}, l.getFields().get(1));  // empty because "L|1||1|31"
		assertArrayEquals(new String[]{"1"}, l.getFields().get(2));
		assertArrayEquals(new String[]{"31"}, l.getFields().get(3));
	}

	@Test
	void parseData_shouldThrowInternalErrorException_onInvalidBase64() {
		assertThrows(InternalErrorException.class, () -> HPRIMDataReader.parseData("%%%NOT_BASE64%%%"));
	}
}
