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

public class MapperTestAppend {

	private final String hl7v2Message =
		"MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20120411070545||ORU^R01|59689|P|2.3\r"
			+ "PID|1|PID21^PID22^PID23^Dx Care&1.2.250.1.38.3.1.101&ISO^PT|PID31^PID32^PID33^MIE&1.2.840.114398.1.100&ISO^MR|PID41^PID22^PID43^PID441&1.2.3.4.5&HCD^SS|MOUSE^MINNIE^S||19240101|F|ALIASMOUSE^ALIASMINNIE||123 MOUSEHOLE LN^PID112^FORT WAYNE^IN^46808^USA^H^^ALLEN^001234||(260)555-1234^PRN^PH^minnie.mouse@example.org^1^260^5551234^99^Home phone|(260)555-9876^WPN^PH^minnie.work@example.org^1^260^5559876^123^Work phone|||||999-88-7777|D123456789^1.2.250.1.38.3.1.102^20300101||||||||||||\r"
			+ "PV1|1|O|||||71^DUCK^DONALD||||||||||||12376|||||||||||||||||||||||||20120410160227||||||\r"
			+ "ORC|RE||12376|||||||100^DUCK^DASIY||71^DUCK^DONALD|^^^||20120411070545|||||\r"
			+ "OBR|1|PLACER12376^OE^1.2.3.4.5^ISO|FILLER98765^LAB^9.8.7.6.5^HL7|cbc^CBC^R|R|198703281530|20120410160227|||22^GOOF^GOOFY^^^^MD^L|||Fasting: No|20120410162500|BLD|71^DUCK^DONALD^^^^DR.|(555)555-1234|||||||20120410163000|||F||^^^^^R|||||||||||||||||||85025|\r"
			+ "OBX|1|SN|wbc^Wbc^Local^6690-2^Wbc^LN|1|<>^12^3^5|/nl|3.8-11.0|N|0.95|A|F|20120410|SECRET|20120410160227|lab|12^XYZ LAB|\r";

	private ValidationSupportChain validationSupport;
	private IWorkerContext hapiContext;
	private IFhirResourceDao<StructureMap> structureMapDao;

	@Test
	void mapHL7v2ToFHIRTestAppend() {
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
			.setResource(new Binary().setContentType("text/x-hl7-ft").setContentAsBase64(Base64.encode(hl7v2Message.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		Parameters result = mapper.map(getStructureMapInstant(), parameters);

		assertNotNull(result);
		assertNotNull(result.getParameter("target").getResource());
		assertInstanceOf(Binary.class, result.getParameter("target").getResource());
		Observation observation = (Observation) context.newJsonParser().parseResource(new ByteArrayInputStream(((Binary) result.getParameter("target").getResource()).getContent()));
		assertEquals("<> 12 3 5/nl", ((StringType) observation.getValue()).getValue());
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
		inputTarget.setType("Observation");
		inputTarget.setMode(StructureMap.StructureMapInputMode.TARGET);

		group.addInput(inputSource);
		group.addInput(inputTarget);

		StructureMap.StructureMapGroupRuleComponent rule1 = group.addRule().setName("appendSN1");
		rule1.addSource()
			.setContext("source").setType("string")
			.setElement("OBX-5-1").setVariable("valueStringSN1");

		StructureMap.StructureMapGroupRuleComponent rule2 = rule1.addRule().setName("appendSN2");
		rule2.addSource()
			.setContext("source").setType("string")
			.setElement("OBX-5-2").setVariable("valueStringSN2");

		StructureMap.StructureMapGroupRuleComponent rule3 = rule2.addRule().setName("appendSN3");
		rule3.addSource()
			.setContext("source").setType("string")
			.setElement("OBX-5-3").setVariable("valueStringSN3");

		StructureMap.StructureMapGroupRuleComponent rule4 = rule3.addRule().setName("appendSN4");
		rule4.addSource()
			.setContext("source").setType("string")
			.setElement("OBX-5-4").setVariable("valueStringSN4");

		StructureMap.StructureMapGroupRuleComponent rule5 = rule4.addRule().setName("appendSNUnit");
		rule5.addSource()
			.setContext("source").setType("string")
			.setElement("OBX-6").setVariable("valueStringSNUnit");

		rule5.addTarget()
			.setContext("target")
			.setContextType(StructureMap.StructureMapContextType.VARIABLE)
			.setElement("value")
			.setTransform(StructureMap.StructureMapTransform.APPEND)
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("valueStringSN1")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType(" ")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("valueStringSN2")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType(" ")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("valueStringSN3")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType(" ")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("valueStringSN4")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("valueStringSNUnit")));

		structureMap.addGroup(group);
		return structureMap;
	}
}
