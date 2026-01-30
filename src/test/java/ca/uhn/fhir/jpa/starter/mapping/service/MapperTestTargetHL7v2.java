package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.starter.mapping.model.HL7v2Builder;
import ca.uhn.fhir.jpa.starter.mapping.model.HPRIMSegment;
import ca.uhn.fhir.jpa.starter.mapping.model.MappingContext;
import ca.uhn.fhir.jpa.starter.mapping.model.Variables;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.StructureMap;
import org.hl7.fhir.r4.utils.StructureMapUtilities;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MapperTestTargetHL7v2 {

	private static Method processTargetMethod() throws Exception {
		Method m = Mapper.class.getDeclaredMethod("processTarget", MappingContext.class, boolean.class);
		m.setAccessible(true);
		return m;
	}

	private Mapper newMapper(FHIRPathEngine fhirPathEngine) {
		IWorkerContext worker = mock(IWorkerContext.class);
		StructureMapUtilities.ITransformerServices services = mock(StructureMapUtilities.ITransformerServices.class);
		@SuppressWarnings("unchecked")
		IFhirResourceDao<StructureMap> dao = mock(IFhirResourceDao.class);
		IGenericClient client = mock(IGenericClient.class);
		return new Mapper(worker, fhirPathEngine, services, dao, client);
	}

	@Test
	void processTarget_hl7v2_shouldAddSegment_whenElementHasNoDash_andSourceIsSegment() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		// Target: HL7 builder stored in OUTPUT context "out"
		HL7v2Builder hl7 = mock(HL7v2Builder.class);

		StructureMap.StructureMapGroupRuleTargetComponent target = new StructureMap.StructureMapGroupRuleTargetComponent();
		target.setContext("out");
		target.setElement("MSH"); // no '-'

		// Rule: source variable name used to fetch INPUT variable
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource().setVariable("srcVar");

		// Source object: a single HPRIM segment
		HPRIMSegment seg = new HPRIMSegment("PID");
		seg.addField(new String[]{"A"});
		seg.addField(new String[]{"B"});

		Variables vars = mock(Variables.class);
		when(vars.get(any(), eq("out"))).thenReturn(hl7);          // OUTPUT/out => HL7v2Builder
		when(vars.get(any(), eq("srcVar"))).thenReturn(seg);       // INPUT/srcVar => segment

		MappingContext ctx = mock(MappingContext.class);
		when(ctx.getTarget()).thenReturn(target);
		when(ctx.getRule()).thenReturn(rule);
		when(ctx.getVariables()).thenReturn(vars);

		processTargetMethod().invoke(mapper, ctx, true);

		verify(hl7).addSegment(eq("PID"), same(seg.getFields()));
		verify(hl7, never()).putByPath(anyString(), anyString());
		verify(vars, never()).add(any(), anyString(), any());
	}

	@Test
	void processTarget_hl7v2_shouldAddAllSegments_whenElementHasNoDash_andSourceIsList() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		HL7v2Builder hl7 = mock(HL7v2Builder.class);

		StructureMap.StructureMapGroupRuleTargetComponent target = new StructureMap.StructureMapGroupRuleTargetComponent();
		target.setContext("out");
		target.setElement("MSH"); // no '-'

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource().setVariable("srcVar");

		HPRIMSegment seg1 = new HPRIMSegment("OBR");
		seg1.addField(new String[]{"1"});
		HPRIMSegment seg2 = new HPRIMSegment("OBX");
		seg2.addField(new String[]{"2"});

		Variables vars = mock(Variables.class);
		when(vars.get(any(), eq("out"))).thenReturn(hl7);
		when(vars.get(any(), eq("srcVar"))).thenReturn(List.of(seg1, seg2));

		MappingContext ctx = mock(MappingContext.class);
		when(ctx.getTarget()).thenReturn(target);
		when(ctx.getRule()).thenReturn(rule);
		when(ctx.getVariables()).thenReturn(vars);

		processTargetMethod().invoke(mapper, ctx, true);

		verify(hl7).addSegment(eq("OBR"), same(seg1.getFields()));
		verify(hl7).addSegment(eq("OBX"), same(seg2.getFields()));
		verify(hl7, never()).putByPath(anyString(), anyString());
	}

	@Test
	void processTarget_hl7v2_shouldPutEmptyString_whenElementHasDash_andNoTransform() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		HL7v2Builder hl7 = mock(HL7v2Builder.class);

		StructureMap.StructureMapGroupRuleTargetComponent target = new StructureMap.StructureMapGroupRuleTargetComponent();
		target.setContext("out");
		target.setElement("OBX-5"); // has '-'
		// no transform

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource().setVariable("srcVar");

		Variables vars = mock(Variables.class);
		when(vars.get(any(), eq("out"))).thenReturn(hl7);
		when(vars.get(any(), eq("srcVar"))).thenReturn(new Object()); // irrelevant for '-' branch

		MappingContext ctx = mock(MappingContext.class);
		when(ctx.getTarget()).thenReturn(target);
		when(ctx.getRule()).thenReturn(rule);
		when(ctx.getVariables()).thenReturn(vars);

		processTargetMethod().invoke(mapper, ctx, false);

		verify(hl7).putByPath(eq("OBX-5"), eq(""));
		verify(hl7, never()).addSegment(anyString(), anyList());
	}

	@Test
	void processTarget_hl7v2_shouldPutTransformValue_andSetOutputVariable_whenHasVariable() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		HL7v2Builder hl7 = mock(HL7v2Builder.class);

		StructureMap.StructureMapGroupRuleTargetComponent target = new StructureMap.StructureMapGroupRuleTargetComponent();
		target.setContext("out");
		target.setElement("OBX-5"); // has '-'
		target.setTransform(StructureMap.StructureMapTransform.COPY);
		target.setVariable("tgtVar");

		// COPY uses parameter[0]. If it is not an IdType, it is returned as-is.
		target.addParameter().setValue(new StringType("VAL"));

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource().setVariable("srcVar");

		Variables vars = mock(Variables.class);
		when(vars.get(any(), eq("out"))).thenReturn(hl7);
		when(vars.get(any(), eq("srcVar"))).thenReturn(new Object());

		MappingContext ctx = mock(MappingContext.class);
		when(ctx.getTarget()).thenReturn(target);
		when(ctx.getRule()).thenReturn(rule);
		when(ctx.getVariables()).thenReturn(vars);

		processTargetMethod().invoke(mapper, ctx, true);

		verify(hl7).putByPath(eq("OBX-5"), eq("VAL"));
		verify(vars).add(any(), eq("tgtVar"), any(StringType.class));
	}

	@Test
	void processTarget_shouldThrow_whenTargetContextUnknown() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		StructureMap.StructureMapGroupRuleTargetComponent target = new StructureMap.StructureMapGroupRuleTargetComponent();
		target.setContext("out");
		// missing element => hasElement() false triggers exception when context is used

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");

		Variables vars = mock(Variables.class);
		when(vars.get(any(), eq("out"))).thenReturn(null); // unknown context

		MappingContext ctx = mock(MappingContext.class);
		when(ctx.getTarget()).thenReturn(target);
		when(ctx.getRule()).thenReturn(rule);
		when(ctx.getVariables()).thenReturn(vars);

		InvocationTargetException ite = assertThrows(
			InvocationTargetException.class,
			() -> processTargetMethod().invoke(mapper, ctx, true)
		);

		assertInstanceOf(InvalidRequestException.class, ite.getCause());
	}
}
