package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.DefaultProfileValidationSupport;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import org.apache.jena.ext.xerces.impl.dv.util.Base64;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.common.hapi.validation.support.ValidationSupportChain;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.hapi.ctx.HapiWorkerContext;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MapperTestTranslate {
	private final String hl7v2Message =
		"MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20120411070545||ORU^R01|59689|P|2.3\r"
			+ "OBX|1|TM|K^Wbc^Local^6690-2^Wbc^LN|1|105432|/nl|3.8-11.0|N|0.95|A|F|20120410|SECRET|20120410160227|lab|12^XYZ LAB|\r";

	private ValidationSupportChain validationSupport;
	private IWorkerContext hapiContext;
	private IFhirResourceDao<StructureMap> structureMapDao;

	@Test
	void mapHL7v2ToFHIRTestTranslateNoContained() {
		FhirContext context = FhirContext.forR4();
		PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
		this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

		this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

		IGenericClient clientStructureMap = null;
		Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null, structureMapDao, clientStructureMap);

		// Transform parameters
		Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
		param.setName("input");

		param.addPart(new Parameters.ParametersParameterComponent().setName("source")
			.setResource(new Binary().setContentType("text/x-hl7-ft").setContentAsBase64(Base64.encode(hl7v2Message.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		// Transform operation
		FHIRException e = assertThrows(FHIRException.class, () -> mapper.map(getStructureMap("#NotContained"), parameters));
		assertEquals(e.getMessage(),"Exception executing transform ObservationTarget.code.coding[+].code = translate(obs3Identifier, '#NotContained', 'code') on Rule \"obx131Translate\": Unable to translate - cannot find map #NotContained");
	}

	@Test
	void mapHL7v2ToFHIRTestTranslateContained() {
		FhirContext context = FhirContext.forR4();
		PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
		this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

		this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

		IGenericClient clientStructureMap = null;
		Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null, structureMapDao, clientStructureMap);

		// Transform parameters
		Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
		param.setName("input");

		param.addPart(new Parameters.ParametersParameterComponent().setName("source")
			.setResource(new Binary().setContentType("text/x-hl7-ft").setContentAsBase64(Base64.encode(hl7v2Message.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		// Transform operation
		Parameters result =  mapper.map(getStructureMapContained(), parameters);
		assertNotNull(result.getParameter("ObservationTarget").getResource());
		assertTrue(result.getParameter("ObservationTarget").getResource() instanceof Binary);
		Observation observation = (Observation) context.newJsonParser().parseResource(new ByteArrayInputStream(((Binary) result.getParameter("ObservationTarget").getResource()).getContent()));
		assertEquals("2823-3", observation.getCode().getCoding().get(0).getCode());
	}

	@Test
	void mapHL7v2ToFHIRTestTranslateLocal() {
		FhirContext context = FhirContext.forR4();
		PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
		this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

		this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

		//Mock the worker for CM retrieve
		IWorkerContext worker = mock(IWorkerContext.class);

		when(worker.fetchResource(any(), any())).thenReturn(getConceptMap());

		IGenericClient clientStructureMap = null;
		Mapper mapper = new Mapper(worker, fhirPathEngine, null, structureMapDao, clientStructureMap);

		// Transform parameters
		Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
		param.setName("input");

		param.addPart(new Parameters.ParametersParameterComponent().setName("source")
			.setResource(new Binary().setContentType("text/x-hl7-ft").setContentAsBase64(Base64.encode(hl7v2Message.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		// Transform operation
		Parameters result =  mapper.map(getStructureMap(), parameters);

		// Asserts
		assertNotNull(result.getParameter("ObservationTarget").getResource());
		assertTrue(result.getParameter("ObservationTarget").getResource() instanceof Binary);
		Observation observation = (Observation) context.newJsonParser().parseResource(new ByteArrayInputStream(((Binary) result.getParameter("ObservationTarget").getResource()).getContent()));
		assertEquals("2823-3", observation.getCode().getCoding().get(0).getCode());

		verify(worker).fetchResource(eq(ConceptMap.class), eq("http://fyrstain.com/fhir/R4/okeiro-ig/ConceptMap/CM-CHUGA-blood-LabToLoinc"));
	}

	@Test
	void mapHL7v2ToFHIRTestTranslateRemote() {
		FhirContext context = FhirContext.forR4();
		PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
		this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

		this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

		//Mock the worker for CM retrieve (should not be used)
		IWorkerContext worker = mock(IWorkerContext.class);
		when(worker.fetchResource(any(), any())).thenReturn(getConceptMap());

		//Mock the translate service client
		TransformerService transformerService = mock(TransformerService.class);
		when(transformerService.translate(any(), any(), any())).thenReturn(new Coding("https://loinc.org", "2823-3", ""));

		IGenericClient clientStructureMap = null;
		Mapper mapper = new Mapper(worker, fhirPathEngine, transformerService, structureMapDao, clientStructureMap);

		// Transform parameters
		Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
		param.setName("input");

		param.addPart(new Parameters.ParametersParameterComponent().setName("source")
			.setResource(new Binary().setContentType("text/x-hl7-ft").setContentAsBase64(Base64.encode(hl7v2Message.getBytes()))));

		Parameters parameters = new Parameters().addParameter(param);

		// Transform operation
		Parameters result =  mapper.map(getStructureMap(), parameters);

		// Asserts
		assertNotNull(result.getParameter("ObservationTarget").getResource());
		assertTrue(result.getParameter("ObservationTarget").getResource() instanceof Binary);
		Observation observation = (Observation) context.newJsonParser().parseResource(new ByteArrayInputStream(((Binary) result.getParameter("ObservationTarget").getResource()).getContent()));
		assertEquals("2823-3", observation.getCode().getCoding().get(0).getCode());

		verify(worker, never()).fetchResource(eq(ConceptMap.class), eq("http://fyrstain.com/fhir/R4/okeiro-ig/ConceptMap/CM-CHUGA-blood-LabToLoinc"));
		ArgumentCaptor<Coding> codingArgumentCaptor = ArgumentCaptor.forClass(Coding.class);
		verify(transformerService).translate(eq(null),  codingArgumentCaptor.capture(),eq("http://fyrstain.com/fhir/R4/okeiro-ig/ConceptMap/CM-CHUGA-blood-LabToLoinc"));
		assertEquals(codingArgumentCaptor.getValue().getCode(), "K");
	}

	private StructureMap getStructureMapContained() {
		StructureMap map = getStructureMap("#CM-CHUGA-blood-LabToLoinc");
		map.addContained(getConceptMap());
		return map;
	}

	private ConceptMap getConceptMap() {
		ConceptMap cm = new ConceptMap();
		cm.setId("#CM-CHUGA-blood-LabToLoinc");
		cm.setUrl("http://fyrstain.com/fhir/R4/okeiro-ig/ConceptMap/CM-CHUGA-blood-LabToLoinc");

		cm.setStatus(Enumerations.PublicationStatus.DRAFT);

		// Group
		ConceptMap.ConceptMapGroupComponent group = cm.addGroup();
		group.setSource("http://fyrstain.com/fhir/R4/okeiro-ig/CodeSystem/COS-CHUGA-LabAnalyses");
		group.setTarget("http://loinc.org");

		// Element (single source code)
		ConceptMap.SourceElementComponent element = group.addElement();
		element.setCode("K");
		element.setDisplay("K");

		// Target (single target code)
		ConceptMap.TargetElementComponent tgt = element.addTarget();
		tgt.setCode("2823-3");
		tgt.setEquivalence(Enumerations.ConceptMapEquivalence.EQUIVALENT);
		tgt.setComment("marker=potassium; origin=mirth(mappingAnalyse); type=blood");
		return cm;
	}

	private StructureMap getStructureMap() {
		return getStructureMap("http://fyrstain.com/fhir/R4/okeiro-ig/ConceptMap/CM-CHUGA-blood-LabToLoinc");
	}

	private StructureMap getStructureMap(String smReference) {
		// ------------------------------------------------------------
		// StructureMap root
		// ------------------------------------------------------------
		StructureMap map = new StructureMap();
		map.setUrl("http://fyrstain.com/fhir/R4/okeiro-ig/StructureMap/CHUGA-OBX-to-LabReport-Observation");

		// ------------------------------------------------------------
		// Structure definitions
		// ------------------------------------------------------------
		map.addStructure()
			.setUrl("https://fyrstain.com")
			.setMode(StructureMap.StructureMapModelMode.SOURCE);

		map.addStructure()
			.setUrl("http://hl7.eu/fhir/laboratory/StructureDefinition/Observation-resultslab-eu-lab")
			.setMode(StructureMap.StructureMapModelMode.TARGET)
			.setAlias("ObservationTarget");

		// ------------------------------------------------------------
		// Group: main
		// ------------------------------------------------------------
		StructureMap.StructureMapGroupComponent mainGroup = map.addGroup();
		mainGroup.setName("main");
		mainGroup.setTypeMode(StructureMap.StructureMapGroupTypeMode.TYPES);

		// Inputs
		mainGroup.addInput()
			.setName("source")
			.setType("HL7v2")
			.setMode(StructureMap.StructureMapInputMode.SOURCE);

		mainGroup.addInput()
			.setName("ObservationTarget")
			.setType("Observation")
			.setMode(StructureMap.StructureMapInputMode.TARGET);

		// ------------------------------------------------------------
		// Rule: labReportObservationResource
		// ------------------------------------------------------------
		StructureMap.StructureMapGroupRuleComponent labRule = mainGroup.addRule();
		labRule.setName("labReportObservationResource");

		labRule.addSource()
			.setContext("source")
			.setType("string")
			.setElement("OBX")
			.setVariable("obs")
			.setCondition("$this.exists()");

		// ------------------------------------------------------------
		// Nested Rule: obx3ToObservationCode
		// ------------------------------------------------------------
		StructureMap.StructureMapGroupRuleComponent obx3Rule = labRule.addRule();
		obx3Rule.setName("obx3ToObservationCode");

		obx3Rule.addSource()
			.setContext("obs")
			.setType("string")
			.setElement("OBX-3")
			.setVariable("observationCode")
			.setCondition("$this.exists()");

		// ------------------------------------------------------------
		// Nested Rule: obx131
		// ------------------------------------------------------------
		StructureMap.StructureMapGroupRuleComponent obx131Rule = obx3Rule.addRule();
		obx131Rule.setName("obx131");

		obx131Rule.addSource()
			.setContext("obs")
			.setType("string")
			.setElement("OBX-3-1")
			.setVariable("obs3Identifier")
			.setCondition("$this.exists()");

		// ------------------------------------------------------------
		// Nested Rule: obx131Translate
		// ------------------------------------------------------------
		StructureMap.StructureMapGroupRuleComponent translateRule = obx131Rule.addRule();
		translateRule.setName("obx131Translate");

		translateRule.addSource()
			.setContext("obs")
			.setType("string")
			.setElement("OBX-3-1")
			.setVariable("obs3Identifier")
			.setCondition("$this.exists()");

		// Target: translate into ObservationTarget.code.coding[+].code
		StructureMap.StructureMapGroupRuleTargetComponent target = translateRule.addTarget();
		target.setContext("ObservationTarget");
		target.setElement("code.coding[+].code");
		target.setTransform(StructureMap.StructureMapTransform.TRANSLATE);

		// Parameters for translate()
		target.addParameter()
			.setValue(new IdType("obs3Identifier"));

		target.addParameter()
			.setValue(new org.hl7.fhir.r4.model.StringType(smReference));

		target.addParameter()
			.setValue(new org.hl7.fhir.r4.model.StringType("code"));

		return map;
	}
}
