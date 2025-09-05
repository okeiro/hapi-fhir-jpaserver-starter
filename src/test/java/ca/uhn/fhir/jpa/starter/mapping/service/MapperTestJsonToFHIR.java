package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.DefaultProfileValidationSupport;
import org.apache.jena.ext.xerces.impl.dv.util.Base64;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.common.hapi.validation.support.ValidationSupportChain;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.hapi.ctx.HapiWorkerContext;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StructureMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MapperTestJsonToFHIR {
    private ValidationSupportChain validationSupport;
    private IWorkerContext hapiContext;
    private final String json = "{\"firstName\": [\"John\", \"Jack\"],\"lastName\": \"Doe\"}";

    @Test
    void mapJSONToFHIR() {
        FhirContext context = FhirContext.forR4();
        PrePopulatedValidationSupport prePopulatedValidationSupport = new PrePopulatedValidationSupport(context);
        this.validationSupport = new ValidationSupportChain(prePopulatedValidationSupport, new DefaultProfileValidationSupport(context));

        this.hapiContext = new HapiWorkerContext(context, this.validationSupport);// Init the Hapi Work

        FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);

        Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null);

        Parameters.ParametersParameterComponent param = new Parameters.ParametersParameterComponent();
        param.setName("input");

        param.addPart(new Parameters.ParametersParameterComponent().setName("source")
                .setResource(new Binary().setContentType("application/json").setContentAsBase64(Base64.encode(json.getBytes()))));

        Parameters parameters = new Parameters().addParameter(param);

        Parameters result = mapper.map(getStructureMap(), parameters);

        assertNotNull(result);
        assertNotNull(result.getParameter("target").getResource());
        assertTrue(result.getParameter("target").getResource() instanceof Binary);
    }

    private StructureMap getStructureMap() {
        StructureMap structureMap = new StructureMap();

        StructureMap.StructureMapGroupComponent group = new StructureMap.StructureMapGroupComponent();
        group.setName("main");
        group.setTypeMode(StructureMap.StructureMapGroupTypeMode.NONE);

        StructureMap.StructureMapGroupInputComponent inputSource = new StructureMap.StructureMapGroupInputComponent();
        inputSource.setName("source");
        inputSource.setType("JSON");
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
        sourceFirstName.setElement("$.firstName");
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
