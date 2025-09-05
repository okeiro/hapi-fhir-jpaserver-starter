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

class MapperTestCSVToFHIR {

    private ValidationSupportChain validationSupport;
    private IWorkerContext hapiContext;

    private final String csv = "column1;column2;column3\n" +
            "1;text;20/12/2023";

    @Test
    void mapCSVToFHIR() {
        FhirContext context = FhirContext.forR4();
        PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
        this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

        this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

        FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

        Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null);

        Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
        param.setName("input");

        param.addPart(new Parameters.ParametersParameterComponent().setName("source")
                .setResource(new Binary().setContentType("text/csv").setContentAsBase64(Base64.encode(csv.getBytes()))));

        Parameters parameters = new Parameters().addParameter(param);

        Parameters result = mapper.map(getStructureMap(), parameters);

        assertNotNull(result);
        assertNotNull(result.getParameter("target").getResource());
        assertTrue(result.getParameter("target").getResource() instanceof Binary);
        Binary target = (Binary) result.getParameter("target").getResource();
        String content = new String(target.getContent(), StandardCharsets.UTF_8);

        IBaseResource resource = context.newJsonParser().parseResource(content);
        assertTrue(resource instanceof Patient);
        Patient patient = (Patient) resource;

        assertEquals("1", patient.getIdPart());
        assertEquals("text", patient.getNameFirstRep().getGiven().get(0).getValue());
        assertEquals(1703026800000L, patient.getBirthDate().getTime());
    }

    private StructureMap getStructureMap() {
        StructureMap structureMap = new StructureMap();

        StructureMap.StructureMapGroupComponent group = structureMap.addGroup();
        group.addInput().setName("source").setType("CSV").setMode(StructureMap.StructureMapInputMode.SOURCE);
        group.addInput().setName("target").setType("Patient").setMode(StructureMap.StructureMapInputMode.TARGET);

        StructureMap.StructureMapGroupRuleComponent rule = group.addRule().setName("CreateRule");
        rule.addTarget().setVariable("target").setTransform(StructureMap.StructureMapTransform.CREATE)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent().setValue(new StringType("Observation")));

        StructureMap.StructureMapGroupRuleComponent idRule = group.addRule().setName("IDRule");
        idRule.addSource().setContext("source").setType("string").setElement("column1").setVariable("variable1");
        idRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
                .setElement("id").setTransform(StructureMap.StructureMapTransform.CAST)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("variable1")))
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new StringType("id")));

        StructureMap.StructureMapGroupRuleComponent nameRule = group.addRule().setName("NameRule");
        nameRule.addSource().setContext("source").setType("string").setElement("column2").setVariable("variable2");
        nameRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
                .setElement("name.given").setTransform(StructureMap.StructureMapTransform.COPY)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("variable2")));

        StructureMap.StructureMapGroupRuleComponent dobRule = group.addRule().setName("dobRule");
        dobRule.addSource().setContext("source").setType("string").setElement("column3").setVariable("variable3");
        dobRule.addTarget().setContext("target").setContextType(StructureMap.StructureMapContextType.VARIABLE)
                .setElement("birthDate").setTransform(StructureMap.StructureMapTransform.DATEOP)
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new IdType("variable3")))
                .addParameter(new StructureMap.StructureMapGroupRuleTargetParameterComponent()
                        .setValue(new StringType("dd/MM/yyyy")));

        return structureMap;
    }
}