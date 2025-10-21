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

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class MapperTestFHIRToCSV {

    private ValidationSupportChain validationSupport;
    private IWorkerContext hapiContext;
	private IFhirResourceDao<StructureMap> structureMapDao;

    @Test
    void mapCSVToFHIR() {
        Patient source = new Patient();
        source.addName().setText("Test").setFamily("1");
        source.addName().setText("Tartopom").setFamily("2");
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
                .setResource(new Binary().setContentType("text/csv").setContentAsBase64(Base64.encode(context.newJsonParser().encodeResourceToString(source).getBytes()))));

        Parameters parameters = new Parameters().addParameter(param);

        Parameters result = mapper.map(getStructureMap(), parameters);

        assertNotNull(result);
        assertNotNull(result.getParameter("target").getResource());
        assertTrue(result.getParameter("target").getResource() instanceof Binary);
        Binary target = (Binary) result.getParameter("target").getResource();
        String content = new String(target.getContent(), StandardCharsets.UTF_8);

        assertEquals("1,2\n" +
                "Test,Tartopom\n", content);
    }

    private StructureMap getStructureMap() {
        StructureMap structureMap = new StructureMap();
		 structureMap.setUrl("http://example.org/base");

        StructureMap.StructureMapGroupComponent group = structureMap.addGroup();
        group.addInput().setName("source").setType("Patient").setMode(StructureMap.StructureMapInputMode.SOURCE);
        group.addInput().setName("target").setType("CSV").setMode(StructureMap.StructureMapInputMode.TARGET);

        StructureMap.StructureMapGroupRuleComponent nameRule = group.addRule().setName("nameRule");
        nameRule.addSource().setContext("source").setElement("name").setVariable("names");

        StructureMap.StructureMapGroupRuleComponent idRule = nameRule.addRule().setName("IDRule");
        idRule.addSource().setContext("names").setType("string").setElement("family").setVariable("family");
        idRule.addTarget().setContext("target")
                .setElement("header").setTransform(StructureMap.StructureMapTransform.COPY)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("family")));

        StructureMap.StructureMapGroupRuleComponent valueRule = idRule.addRule().setName("ValueRule");
        valueRule.addSource().setContext("names").setType("string").setElement("text").setVariable("name");
        valueRule.addTarget().setContext("target")
                .setElement("family").setTransform(StructureMap.StructureMapTransform.CAST)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("name")))
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new StringType("string")));

        return structureMap;
    }
}