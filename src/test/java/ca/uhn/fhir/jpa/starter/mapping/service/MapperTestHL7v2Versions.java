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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.*;

public class MapperTestHL7v2Versions {

	private ValidationSupportChain validationSupport;
	private IWorkerContext hapiContext;
	private IFhirResourceDao<StructureMap> structureMapDao;

	@ParameterizedTest
	@ValueSource(strings = {"2.2", "2.3", "2.3.1","2.4", "2.5", "2.5.1", "2.6", "2.7", "2.7.1", "2.8", "2.8.1"})
	void mapHL7v2ToFHIRVersions(String version) throws Exception {
		FhirContext context = FhirContext.forR4();
		PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
		this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport,
			new DefaultProfileValidationSupport(context));
		this.hapiContext = new HapiWorkerContext(context, this.validationSupport);

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

		IGenericClient clientStructureMap = null;
		Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null, structureMapDao, clientStructureMap);

		String hl7v2Message = String.format(
			"MSH|^~\\&|HIS|RIH|EKG|EKG|199904140038||ADT^A01||P|%s\r" +
				"PID|0001|00009874|00001122|A00977|SMITH^JOHN^M|MOM|19581119|F|NOTREAL^LINDA^M|C|564 SPRING ST^^NEEDHAM^MA^02494^US|0002|(818)565-1551|(425)828-3344|E|S|C|0000444444|252-00-4414||||SA|||SA||||NONE|V1|0001||D.ER^50A^M110^01|ER|P00055|11B^M011^02|070615^BATMAN^GEORGE^L|555888^NOTREAL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^NOTREAL^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|199904101200||||5555112333|||666097^NOTREAL^MANNY^P\r" +
				"NK1|0222555|NOTREAL^JAMES^R|FA|STREET^OTHER STREET^CITY^ST^55566|(222)111-3333|(888)999-0000|||||||ORGANIZATION",
			version
		);

		Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
		param.setName("input");
		param.addPart(new Parameters.ParametersParameterComponent()
			.setName("source")
			.setResource(new Binary()
				.setContentType("text/x-hl7-ft")
				.setContentAsBase64(Base64.encode(hl7v2Message.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		Parameters result = mapper.map(getStructureMap(), parameters);

		assertNotNull(result);
		assertNotNull(result.getParameter("target").getResource());
		assertTrue(result.getParameter("target").getResource() instanceof Binary);

		Patient patient = (Patient) context.newJsonParser().parseResource(
			new ByteArrayInputStream(((Binary) result.getParameter("target").getResource()).getContent())
		);

		assertEquals("JAMES", patient.getName().get(0).getGiven().get(0).getValue(),
			"First name must be extracted correctly for HL7v2 " + version);
	}

	private StructureMap getStructureMap() {
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

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("name");

		StructureMap.StructureMapGroupRuleSourceComponent sourceFirstName = new StructureMap.StructureMapGroupRuleSourceComponent();
		sourceFirstName.setContext("source");
		sourceFirstName.setElement("NK1-2-2");
		sourceFirstName.setMin(1);
		sourceFirstName.setMax("1");
		sourceFirstName.setType("string");
		sourceFirstName.setVariable("firstName0");

		StructureMap.StructureMapGroupRuleTargetComponent targetGivenName = new StructureMap.StructureMapGroupRuleTargetComponent();
		targetGivenName.setContext("target");
		targetGivenName.setContextType(StructureMap.StructureMapContextType.VARIABLE);
		targetGivenName.setElement("name.given");
		targetGivenName.setTransform(StructureMap.StructureMapTransform.COPY);
		targetGivenName.addParameter().setValue(new IdType("firstName0"));

		rule.addSource(sourceFirstName);
		rule.addTarget(targetGivenName);

		group.addRule(rule);
		structureMap.addGroup(group);

		return structureMap;
	}
}
