package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.DefaultProfileValidationSupport;
import org.apache.jena.ext.xerces.impl.dv.util.Base64;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.common.hapi.validation.support.ValidationSupportChain;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.hapi.ctx.HapiWorkerContext;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class MapperTestXMLToFHIR {

    private ValidationSupportChain validationSupport;
    private IWorkerContext hapiContext;

    private final String xml ="<questionnaire><id>Questionnaire-Infectiologie</id><name>Avis de Diagnostic en Infectiologie</name><version>Version 1</version><publisher><name>Fyrstain</name></publisher></questionnaire>";

    @Test
    void mapXMLToFHIR() {
        FhirContext context = FhirContext.forR4();
        PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
        this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

        this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

        FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

        Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null);

        Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
        param.setName("input");

        param.addPart(new Parameters.ParametersParameterComponent().setName("source")
                .setResource(new Binary().setContentType("application/xml").setContentAsBase64(Base64.encode(xml.getBytes()))));

        Parameters parameters = new Parameters().addParameter(param);

        Parameters result = mapper.map(getStructureMap(), parameters);

        assertNotNull(result);
        assertNotNull(result.getParameter("target").getResource());
        assertTrue(result.getParameter("target").getResource() instanceof Binary);

        Binary target = (Binary) result.getParameter("target").getResource();
        String content = new String(target.getContent(), StandardCharsets.UTF_8);

        IBaseResource resource = context.newJsonParser().parseResource(content);
        assertTrue(resource instanceof Questionnaire);
        Questionnaire questionnaire = (Questionnaire) resource;
        // Test CAST
        assertEquals("Questionnaire-Infectiologie", questionnaire.getIdPart());
        // Test TRUNCATE
        assertEquals("Avis", questionnaire.getName());
        // Test COPY
        assertEquals("Version 1", questionnaire.getVersion());
        // Test CAST Child Element
        assertEquals("Fyrstain", questionnaire.getPublisher());
    }

    private StructureMap getStructureMap() {
        StructureMap structureMap = new StructureMap();

        StructureMap.StructureMapGroupComponent group = structureMap.addGroup();
        group.addInput().setName("source").setType("XML").setMode(StructureMap.StructureMapInputMode.SOURCE);
        group.addInput().setName("target").setType("Questionnaire").setMode(StructureMap.StructureMapInputMode.TARGET);

        StructureMap.StructureMapGroupRuleComponent rule = group.addRule().setName("CreateRule");
        rule.addTarget().setVariable("target").setTransform(StructureMap.StructureMapTransform.CREATE)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent().setValue(new StringType("Questionnaire")));
        // Test CAST
        StructureMap.StructureMapGroupRuleComponent idRule = group.addRule().setName("IDRule");
        idRule.addSource().setContext("source").setType("string").setElement("id").setVariable("variable1");
        idRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
                .setElement("id").setTransform(StructureMap.StructureMapTransform.CAST)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("variable1")))
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new StringType("id")));
        // Test TRUNCATE
        StructureMap.StructureMapGroupRuleComponent nameRule = group.addRule().setName("nameRule");
        nameRule.addSource().setContext("source").setType("string").setElement("name").setVariable("variable1");
        nameRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
                .setElement("name").setTransform(StructureMap.StructureMapTransform.TRUNCATE)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("variable1")))
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IntegerType(4)));

        // Test COPY
        StructureMap.StructureMapGroupRuleComponent versionRule = group.addRule().setName("versionRule");
        versionRule.addSource().setContext("source").setType("string").setElement("version").setVariable("variable1");
        versionRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
                .setElement("version").setTransform(StructureMap.StructureMapTransform.COPY)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("variable1")));

        // Test CAST Child Element
        StructureMap.StructureMapGroupRuleComponent publisherRule = group.addRule().setName("publisherRule");
        StructureMap.StructureMapGroupRuleComponent publisherRuleChild = publisherRule.addRule().setName("publisherRuleChild");
        publisherRule.addSource().setContext("source").setType("json").setElement("publisher").setVariable("namePublisher");
        publisherRuleChild.addSource().setContext("namePublisher").setType("json").setElement("name").setVariable("namePublisherChild");
        publisherRuleChild.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
                .setElement("publisher").setTransform(StructureMap.StructureMapTransform.CAST)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("namePublisherChild")))
        .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                .setValue(new StringType("string")));

        return structureMap;
    }
}
