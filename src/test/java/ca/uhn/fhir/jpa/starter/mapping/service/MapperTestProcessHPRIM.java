package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.starter.mapping.model.HPRIMMessage;
import ca.uhn.fhir.jpa.starter.mapping.model.HPRIMSegment;
import ca.uhn.fhir.jpa.starter.mapping.model.MappingContext;
import ca.uhn.fhir.jpa.starter.mapping.model.Variables;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.fhirpath.ExpressionNode;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.StructureMap;
import org.hl7.fhir.r4.utils.StructureMapUtilities;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MapperTestProcessHPRIM {

	private static StructureMap.StructureMapGroupRuleSourceComponent source(String context, String element, String type) {
		StructureMap.StructureMapGroupRuleSourceComponent s = new StructureMap.StructureMapGroupRuleSourceComponent();
		s.setContext(context);
		s.setElement(element);
		s.setType(type);
		return s;
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
	void processHPRIMSource_shouldThrow_whenMultipleSourcesInRule() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		MappingContext context = mock(MappingContext.class);

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource();
		rule.addSource();
		when(context.getRule()).thenReturn(rule);

		Variables localVariables = mock(Variables.class);

		Method m = Mapper.class.getDeclaredMethod("processHPRIMSource", MappingContext.class, Variables.class);
		m.setAccessible(true);

		InvocationTargetException ex = assertThrows(InvocationTargetException.class,
			() -> m.invoke(mapper, context, localVariables));

		assertInstanceOf(InvalidRequestException.class, ex.getCause());
	}

	@Test
	void processHPRIMSource_shouldThrow_whenInputVariableNotFound() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		MappingContext context = mock(MappingContext.class);

		StructureMap sm = new StructureMap();
		sm.setUrl("http://example/map");
		when(context.getStructureMap()).thenReturn(sm);

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource().setElement("P-1").setType("string");
		when(context.getRule()).thenReturn(rule);

		StructureMap.StructureMapGroupRuleSourceComponent src = new StructureMap.StructureMapGroupRuleSourceComponent();
		src.setContext("src");
		src.setElement("P-1");
		src.setType("string");
		when(context.getSources()).thenReturn(List.of(src));

		Variables localVariables = mock(Variables.class);
		when(localVariables.get(any(), eq("src"))).thenReturn(null);

		Method m = Mapper.class.getDeclaredMethod("processHPRIMSource", MappingContext.class, Variables.class);
		m.setAccessible(true);

		InvocationTargetException ex = assertThrows(InvocationTargetException.class,
			() -> m.invoke(mapper, context, localVariables));

		assertInstanceOf(InvalidRequestException.class, ex.getCause());
	}

	@Test
	void processHPRIMSource_shouldReturnVariablesPerMatchedItem_whenHasVariable() throws Exception {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		MappingContext context = mock(MappingContext.class);

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource().setElement("P").setType("string").setVariable("v");
		when(context.getRule()).thenReturn(rule);

		StructureMap sm = new StructureMap();
		sm.setUrl("http://example/map");
		when(context.getStructureMap()).thenReturn(sm);

		StructureMap.StructureMapGroupRuleSourceComponent src = new StructureMap.StructureMapGroupRuleSourceComponent();
		src.setContext("src");
		src.setElement("P");
		src.setType("string");
		when(context.getSources()).thenReturn(List.of(src));

		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(new HPRIMSegment("P"));
		msg.addSegment(new HPRIMSegment("P"));

		Variables localVariables = mock(Variables.class);
		when(localVariables.get(any(), eq("src"))).thenReturn(msg);

		Variables copy1 = mock(Variables.class);
		Variables copy2 = mock(Variables.class);
		when(localVariables.copy()).thenReturn(copy1, copy2);

		Method m = Mapper.class.getDeclaredMethod("processHPRIMSource", MappingContext.class, Variables.class);
		m.setAccessible(true);

		@SuppressWarnings("unchecked")
		List<Variables> out = (List<Variables>) m.invoke(mapper, context, localVariables);

		assertEquals(2, out.size(), "Should return one Variables object per matched segment");

		// It should add INPUT + variable "v" for each item
		verify(copy1).add(any(), eq("v"), any());
		verify(copy2).add(any(), eq("v"), any());
	}

	@Test
	void processHPRIMSource_shouldReturnEmpty_whenInputIsNotHprim() throws Exception {
		Mapper mapper = newMapper(mock(FHIRPathEngine.class));

		MappingContext context = mock(MappingContext.class);

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		rule.addSource().setElement("P").setType("string").setVariable("v");
		when(context.getRule()).thenReturn(rule);

		StructureMap.StructureMapGroupRuleSourceComponent src = new StructureMap.StructureMapGroupRuleSourceComponent();
		src.setContext("src");
		src.setElement("P");
		src.setType("string");
		when(context.getSources()).thenReturn(List.of(src));

		Variables localVariables = mock(Variables.class);
		when(localVariables.get(any(), eq("src"))).thenReturn("not-hprim");

		Method m = Mapper.class.getDeclaredMethod("processHPRIMSource", MappingContext.class, Variables.class);
		m.setAccessible(true);

		@SuppressWarnings("unchecked")
		List<Variables> out = (List<Variables>) m.invoke(mapper, context, localVariables);

		assertNotNull(out);
		assertTrue(out.isEmpty(), "Non-HPRIM input => no items => empty result");
	}

	@Test
	void processHPRIMObject_shouldReturnAllSegments_whenPathIsSegmentOnly() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(new HPRIMSegment("OBX"));
		msg.addSegment(new HPRIMSegment("OBX"));

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(source("src", "OBX", "string")));

		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);

		List<Object> out = mapper.processHPRIMObject(context, msg);

		assertEquals(2, out.size());
		assertInstanceOf(HPRIMSegment.class, out.get(0));
		assertInstanceOf(HPRIMSegment.class, out.get(1));
	}

	@Test
	void processHPRIMObject_shouldExtractFieldJoined_whenNoExplicitComponent() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMSegment obx = new HPRIMSegment("OBX");
		obx.addField(new String[]{"1"});
		obx.addField(new String[]{"NM"});
		obx.addField(new String[]{"NA", "", "L"});
		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(obx);

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(source("src", "OBX-3", "string")));
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);
		when(context.getVariables()).thenReturn(mock(Variables.class));

		List<Object> out = mapper.processHPRIMObject(context, msg);

		assertEquals(1, out.size());
		assertInstanceOf(StringType.class, out.get(0));
		assertEquals("NA^^L", ((StringType) out.get(0)).getValue(), "When no explicit component, field is joined with '^'");
	}

	@Test
	void processHPRIMObject_shouldExtractSpecificComponent_whenExplicitComponent() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMSegment obx = new HPRIMSegment("OBX");
		obx.addField(new String[]{"1"});
		obx.addField(new String[]{"NM"});
		obx.addField(new String[]{"NA", "", "L"});
		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(obx);

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(source("src", "OBX-3-1", "string")));
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);
		when(context.getVariables()).thenReturn(mock(Variables.class));

		List<Object> out = mapper.processHPRIMObject(context, msg);

		assertEquals(1, out.size());
		assertInstanceOf(StringType.class, out.get(0));
		assertEquals("NA", ((StringType) out.get(0)).getValue());
	}

	@Test
	void processHPRIMObject_shouldExtractSubComponent_whenProvided() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMSegment obx = new HPRIMSegment("OBX");
		obx.addField(new String[]{"1"});
		obx.addField(new String[]{"ST"});
		obx.addField(new String[]{"A&B&C"});
		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(obx);

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(source("src", "OBX-3-1-2", "string")));
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);
		when(context.getVariables()).thenReturn(mock(Variables.class));

		List<Object> out = mapper.processHPRIMObject(context, msg);

		assertEquals(1, out.size());
		assertInstanceOf(StringType.class, out.get(0));
		assertEquals("B", ((StringType) out.get(0)).getValue());
	}

	@Test
	void processHPRIMObject_shouldNotUseDefaultValue_whenGetFhirItemThrows() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMSegment p = new HPRIMSegment("P");
		p.addField(new String[]{"X"});
		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(p);

		StructureMap.StructureMapGroupRuleSourceComponent src = source("src", "P-1", "Attachment");
		src.setDefaultValue(new StringType("DEFAULT"));

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(src));
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);
		when(context.getVariables()).thenReturn(mock(Variables.class));

		List<Object> out = mapper.processHPRIMObject(context, msg);

		assertTrue(out.isEmpty(), "Default value is not applied when an exception occurs in the try block");
	}

	@Test
	void processHPRIMObject_shouldReturnEmpty_whenConditionDoesNotMatch() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		when(fhirPathEngine.parse(anyString())).thenReturn(mock(ExpressionNode.class));
		when(fhirPathEngine.evaluateToBoolean(any(), any(), any(), any(), any())).thenReturn(false);

		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMSegment p = new HPRIMSegment("P");
		p.addField(new String[]{"X"});
		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(p);

		StructureMap.StructureMapGroupRuleSourceComponent src = source("src", "P-1", "string");
		src.setCondition("true()");

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(src));
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);
		when(context.getVariables()).thenReturn(mock(Variables.class));

		List<Object> out = mapper.processHPRIMObject(context, msg);

		assertTrue(out.isEmpty(), "Condition not matching should lead to skip and return empty list");
	}

	@Test
	void processHPRIMObject_shouldIgnoreOutOfBoundsField() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMSegment p = new HPRIMSegment("P");
		p.addField(new String[]{"onlyField1"});
		HPRIMMessage msg = new HPRIMMessage();
		msg.addSegment(p);

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(source("src", "P-5", "string")));
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);

		List<Object> out = mapper.processHPRIMObject(context, msg);

		assertTrue(out.isEmpty());
	}

	@Test
	void processHPRIMObject_shouldReturnMatchingSegment_whenInputIsSegment() {
		FHIRPathEngine fhirPathEngine = mock(FHIRPathEngine.class);
		Mapper mapper = newMapper(fhirPathEngine);

		HPRIMSegment seg = new HPRIMSegment("OBX");
		seg.addField(new String[]{"1"});

		MappingContext context = mock(MappingContext.class);
		when(context.getSources()).thenReturn(List.of(source("src", "OBX", "string")));
		StructureMap.StructureMapGroupRuleComponent rule = new StructureMap.StructureMapGroupRuleComponent();
		rule.setName("ruleX");
		when(context.getRule()).thenReturn(rule);

		List<Object> out = mapper.processHPRIMObject(context, seg);

		assertEquals(1, out.size());
		assertInstanceOf(HPRIMSegment.class, out.get(0));
	}
}
