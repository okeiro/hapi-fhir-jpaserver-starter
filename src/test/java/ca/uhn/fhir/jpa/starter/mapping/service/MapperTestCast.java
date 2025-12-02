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

public class MapperTestCast {

	private final String hl7v2Message =
		"MSH|^~\\&|MedApp|MedFacility|MedPlatform|MedFacility|20250804155110||SIU^S14|MSG123456789|P|2.5.1\r"
			+ "SCH||9999999999^MedPlatform||||PROC1^ProcedureType|||||^^30^202508041530|||||12345678^DUPONT^MARIE||||MedPlatform|||||Cancelled\r"
			+ "NTE|||\r"
			+ "PID|||987654321^^^DxCare&1.2.250.1.38.3.1.101&ISO^PI~299999999999999^^^ASIP-SANTE-NIR&1.2.250.1.213.1.4.8&ISO^NH||DOE^JOHN^^^^^D~DOE^JOHN^^^^^L||19880101|M|||10 RUE EXEMPLE^^VILLEEXEMPLE^^99999~^^^^^^BDL^^99887||~0601020304^PRN^CP^^^^^^^^^0601020304~^NET^^john.doe@example.com||||||||||France\r"
			+ "PV1|1|O\r"
			+ "RGS|1\r"
			+ "AIG|1|||DRTEST^TEST PHYSICIAN\r"
			+ "AIL|1||^||SPECUNIT\r";

	private ValidationSupportChain validationSupport;
	private IWorkerContext hapiContext;
	private IFhirResourceDao<StructureMap> structureMapDao;

	@Test
	void mapHL7v2ToFHIRTestCastPositiveInt() {
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
		Appointment appointment = (Appointment) context.newJsonParser().parseResource(new ByteArrayInputStream(((Binary) result.getParameter("target").getResource()).getContent()));
		assertInstanceOf(PositiveIntType.class, appointment.getMinutesDurationElement());
		assertEquals(30, appointment.getMinutesDurationElement().getValue());
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
		inputTarget.setType("Appointment");
		inputTarget.setMode(StructureMap.StructureMapInputMode.TARGET);

		group.addInput(inputSource);
		group.addInput(inputTarget);

		StructureMap.StructureMapGroupRuleComponent dobRule = group.addRule().setName("dobRule");
		dobRule.addSource().setContext("source").setType("string").setElement("SCH-11-3").setVariable("duration");
		dobRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
			.setElement("minutesDuration").setTransform(StructureMap.StructureMapTransform.CAST)
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new IdType("duration")))
			.addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
				.setValue(new StringType("positiveInt")));
		structureMap.addGroup(group);
		return structureMap;
	}
}
