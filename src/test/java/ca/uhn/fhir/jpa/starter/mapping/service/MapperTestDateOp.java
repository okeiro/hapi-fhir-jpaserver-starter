package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.DefaultProfileValidationSupport;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import org.apache.jena.ext.xerces.impl.dv.util.Base64;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.common.hapi.validation.support.ValidationSupportChain;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.hapi.ctx.HapiWorkerContext;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.*;

public class MapperTestDateOp {
	private final String hl7v2MessageInstant = "MSH|^~\\&|HIS|RIH|EKG|EKG|199904140038||ADT^A01||P|2.2\r"
		+ "PID|0001|00009874|00001122|A00977|SMITH^JOHN^M|MOM|19581119|F|NOTREAL^LINDA^M|C|564 SPRING ST^^NEEDHAM^MA^02494^US|0002|(818)565-1551|(425)828-3344|E|S|C|0000444444|252-00-4414||||SA|||SA||||NONE|V1|0001|I|D.ER^50A^M110^01|ER|P00055|11B^M011^02|070615^BATMAN^GEORGE^L|555888^NOTREAL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^NOTREAL^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|199904101200||||5555112333|||666097^NOTREAL^MANNY^P\r"
		+ "NK1|0222555|NOTREAL^JAMES^R|FA|STREET^OTHER STREET^CITY^ST^55566|(222)111-3333|(888)999-0000|||||||ORGANIZATION\r"
		+ "PV1|0001|I|D.ER^1F^M950^01|ER|P000998|11B^M011^02|070615^BATMAN^GEORGE^L|555888^OKNEL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^VOICE^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|||||5555112333|||666097^DNOTREAL^MANNY^P\r"
		+ "PV2|||0112^TESTING|55555^PATIENT IS NORMAL|NONE|||19990225|19990226|1|1|TESTING|555888^NOTREAL^BOB^K^DR^MD||||||||||PROD^003^099|02|ER||NONE|19990225|19990223|19990316|NONE\r"
		+ "AL1||SEV|001^POLLEN\r"
		+ "GT1||0222PL|NOTREAL^BOB^B||STREET^OTHER STREET^CITY^ST^77787|(444)999-3333|(222)777-5555||||MO|111-33-5555||||NOTREAL GILL N|STREET^OTHER STREET^CITY^ST^99999|(111)222-3333\r"
		+ "IN1||022254P|4558PD|BLUE CROSS|STREET^OTHER STREET^CITY^ST^00990||(333)333-6666||221K|LENIX|||19980515|19990515|||PATIENT01 TEST D||||||||||||||||||02LL|022LP554";

	private final String hl7v2MessageTime =
		"MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20120411070545||ORU^R01|59689|P|2.3\r"
			+ "PID|1|PID21^PID22^PID23^Dx Care&1.2.250.1.38.3.1.101&ISO^PT|PID31^PID32^PID33^MIE&1.2.840.114398.1.100&ISO^MR|PID41^PID22^PID43^PID441&1.2.3.4.5&HCD^SS|MOUSE^MINNIE^S||19240101|F|ALIASMOUSE^ALIASMINNIE||123 MOUSEHOLE LN^PID112^FORT WAYNE^IN^46808^USA^H^^ALLEN^001234||(260)555-1234^PRN^PH^minnie.mouse@example.org^1^260^5551234^99^Home phone|(260)555-9876^WPN^PH^minnie.work@example.org^1^260^5559876^123^Work phone|||||999-88-7777|D123456789^1.2.250.1.38.3.1.102^20300101||||||||||||\r"
			+ "PV1|1|O|||||71^DUCK^DONALD||||||||||||12376|||||||||||||||||||||||||20120410160227||||||\r"
			+ "ORC|RE||12376|||||||100^DUCK^DASIY||71^DUCK^DONALD|^^^||20120411070545|||||\r"
			+ "OBR|1|PLACER12376^OE^1.2.3.4.5^ISO|FILLER98765^LAB^9.8.7.6.5^HL7|cbc^CBC^R|R|198703281530|20120410160227|||22^GOOF^GOOFY^^^^MD^L|||Fasting: No|20120410162500|BLD|71^DUCK^DONALD^^^^DR.|(555)555-1234|||||||20120410163000|||F||^^^^^R|||||||||||||||||||85025|\r"
			+ "OBX|1|TM|wbc^Wbc^Local^6690-2^Wbc^LN|1|105432|/nl|3.8-11.0|N|0.95|A|F|20120410|SECRET|20120410160227|lab|12^XYZ LAB|\r";

	private ValidationSupportChain validationSupport;
	private IWorkerContext hapiContext;
	private IFhirResourceDao<StructureMap> structureMapDao;

	@Test
	void mapHL7v2ToFHIRTestInstant() {
		FhirContext context = FhirContext.forR4();
		PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
		this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

		this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

		IGenericClient clientStructureMap = null;
		Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null, structureMapDao, clientStructureMap);

		Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
		param.setName("input");

		param.addPart(new Parameters.ParametersParameterComponent().setName("source")
			.setResource(new Binary().setContentType("text/x-hl7-ft").setContentAsBase64(Base64.encode(hl7v2MessageInstant.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		Parameters result = mapper.map(getStructureMapInstant(), parameters);

		assertNotNull(result);
		assertNotNull(result.getParameter("target").getResource());
		assertInstanceOf(Binary.class, result.getParameter("target").getResource());
		Patient patient = (Patient) context.newJsonParser().parseResource(new ByteArrayInputStream(((Binary) result.getParameter("target").getResource()).getContent()));
		assertEquals(new InstantType("1999-04-14T00:38:00Z").getValue().toInstant(), patient.getMeta().getLastUpdated().toInstant());
	}

	private StructureMap getStructureMapInstant() {
		StructureMap structureMap = new StructureMap();
		structureMap.setUrl("http://example.org/base");

		StructureMap.StructureMapGroupComponent group = new StructureMap.StructureMapGroupComponent();
		group.setName("main");
		group.setTypeMode(StructureMap.StructureMapGroupTypeMode.NONE);

		StructureMap.StructureMapGroupInputComponent inputSource = new StructureMap.StructureMapGroupInputComponent();
		inputSource.setName("source");
		inputSource.setType("HL7v2");
		inputSource.setMode(StructureMap.StructureMapInputMode.SOURCE);

		StructureMap.StructureMapGroupInputComponent inputTarget = new StructureMap.StructureMapGroupInputComponent();
		inputTarget.setName("target");
		inputTarget.setType("Patient");
		inputTarget.setMode(StructureMap.StructureMapInputMode.TARGET);

		group.addInput(inputSource);
		group.addInput(inputTarget);

		StructureMap.StructureMapGroupRuleComponent dobRule = group.addRule().setName("dobRule");
		dobRule.addSource().setContext("source").setType("string").setElement("MSH-7").setVariable("MSHDateTimeOfMessage");
		dobRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
			.setElement("meta.lastUpdated").setTransform(StructureMap.StructureMapTransform.DATEOP)
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("MSHDateTimeOfMessage")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType("yyyyMMddHHmm")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType("instant")));

		structureMap.addGroup(group);

		return structureMap;
	}

	@Test
	void mapHL7v2ToFHIRTestTime() {
		FhirContext context = FhirContext.forR4();
		PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
		this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

		this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

		IGenericClient clientStructureMap = null;
		Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null, structureMapDao, clientStructureMap);

		Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
		param.setName("input");

		param.addPart(new Parameters.ParametersParameterComponent().setName("source")
			.setResource(new Binary().setContentType("text/x-hl7-ft").setContentAsBase64(Base64.encode(hl7v2MessageTime.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		Parameters result = mapper.map(getStructureMapTime(), parameters);

		assertNotNull(result);
		assertNotNull(result.getParameter("target").getResource());
		assertInstanceOf(Binary.class, result.getParameter("target").getResource());
		Observation observation = (Observation) context.newJsonParser().parseResource(new ByteArrayInputStream(((Binary) result.getParameter("target").getResource()).getContent()));
		assertInstanceOf(TimeType.class, observation.getValue());
		assertEquals("10:54:32", ((TimeType) observation.getValue()).getValue());
	}

	private StructureMap getStructureMapTime() {
		StructureMap structureMap = new StructureMap();
		structureMap.setUrl("http://example.org/base");

		StructureMap.StructureMapGroupComponent group = new StructureMap.StructureMapGroupComponent();
		group.setName("main");
		group.setTypeMode(StructureMap.StructureMapGroupTypeMode.NONE);

		StructureMap.StructureMapGroupInputComponent inputSource = new StructureMap.StructureMapGroupInputComponent();
		inputSource.setName("source");
		inputSource.setType("HL7v2");
		inputSource.setMode(StructureMap.StructureMapInputMode.SOURCE);

		StructureMap.StructureMapGroupInputComponent inputTarget = new StructureMap.StructureMapGroupInputComponent();
		inputTarget.setName("target");
		inputTarget.setType("Observation");
		inputTarget.setMode(StructureMap.StructureMapInputMode.TARGET);

		group.addInput(inputSource);
		group.addInput(inputTarget);

		StructureMap.StructureMapGroupRuleComponent dobRule = group.addRule().setName("dobRule");
		dobRule.addSource().setContext("source").setType("string").setElement("OBX-5").setVariable("obsValueTime");
		dobRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
			.setElement("value").setTransform(StructureMap.StructureMapTransform.DATEOP)
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("obsValueTime")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType("HHmmss")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType("HH:mm:ss")));

		structureMap.addGroup(group);

		return structureMap;
	}
}
