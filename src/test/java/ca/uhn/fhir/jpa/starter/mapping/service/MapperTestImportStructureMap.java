package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.common.hapi.validation.support.ValidationSupportChain;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StructureMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MapperTestImportStructureMap {

	private static FhirContext fhirContext;
	private static String serverBaseUrl;
	@Mock
	private IFhirResourceDao<StructureMap> structureMapDao;
	@Mock
	private IBundleProvider bundleProvider;
	@InjectMocks
	private Mapper mapper;
	private ValidationSupportChain validationSupport;
	private IWorkerContext hapiContext;
	private StructureMap baseMap;
	private StructureMap importedMap;

	@BeforeEach
	void setup() {
		baseMap = new StructureMap();
		baseMap.setUrl("http://example.org/base");

		importedMap = new StructureMap();
		importedMap.setUrl("http://example.org/imported");

		IGenericClient clientStructureMap = null;
		mapper = spy(new Mapper(null, null, null, structureMapDao, clientStructureMap));
	}

	@Test
	void resolveImports_shouldThrowIfUrlIsNull() {
		StructureMap map = new StructureMap();
		assertThrows(InvalidRequestException.class, () -> mapper.resolveImports(map, new HashSet<>()));
	}

	@Test
	void resolveImports_shouldSkipAlreadyVisitedUrl() {
		Set<String> visited = new HashSet<>();
		visited.add("http://example.org/base");

		StructureMap result = mapper.resolveImports(baseMap, visited);
		assertEquals(baseMap, result);
	}

	@Test
	void resolveImports_shouldMergeImportedMap() {
		baseMap.addImport("http://example.org/imported");

		StructureMap.StructureMapGroupComponent importedGroup = new StructureMap.StructureMapGroupComponent();
		importedGroup.setName("ImportedGroup");
		importedMap.getGroup().add(importedGroup);

		doReturn(importedMap).when(mapper).fetchStructureMapByUrl("http://example.org/imported");
		doNothing().when(mapper).mergeStructureMaps(any(), any());

		StructureMap result = mapper.resolveImports(baseMap, new HashSet<>());

		verify(mapper).mergeStructureMaps(any(), any());
		assertNotNull(result);
		assertEquals("http://example.org/base", result.getUrl());
	}

	@Test
	void resolveImports_shouldThrowIfImportNotFound() {
		baseMap.addImport("http://example.org/missing");
		doReturn(null).when(mapper).fetchStructureMapByUrl("http://example.org/missing");

		assertThrows(InvalidRequestException.class, () -> mapper.resolveImports(baseMap, new HashSet<>()));
	}

	@Test
	void fetchStructureMapByUrl_shouldThrowIfNotFound() {
		when(structureMapDao.search(any())).thenReturn(bundleProvider);
		when(bundleProvider.size()).thenReturn(0);

		assertThrows(InvalidRequestException.class, () -> mapper.fetchStructureMapByUrl("http://notfound.org"));
	}

	@Test
	void fetchStructureMapByUrl_shouldReturnFoundMap() {
		when(structureMapDao.search(any())).thenReturn(bundleProvider);
		when(bundleProvider.size()).thenReturn(1);
		when(bundleProvider.getResources(0, 1)).thenReturn(List.of(importedMap));

		StructureMap result = mapper.fetchStructureMapByUrl("http://example.org/imported");
		assertEquals(importedMap, result);
	}

	@Test
	void fetchStructureMapByUrl_shouldWarnIfMultipleResults() {
		when(structureMapDao.search(any())).thenReturn(bundleProvider);
		when(bundleProvider.size()).thenReturn(2);
		when(bundleProvider.getResources(0, 1)).thenReturn(List.of(importedMap));

		StructureMap result = mapper.fetchStructureMapByUrl("http://example.org/imported");
		assertEquals(importedMap, result);
	}

	@Test
	void mergeStructureMaps_shouldAddImportedGroupIfNotExists() {
		StructureMap.StructureMapGroupComponent importedGroup = new StructureMap.StructureMapGroupComponent();
		importedGroup.setName("GroupA");

		importedMap.getGroup().add(importedGroup);

		mapper.mergeStructureMaps(baseMap, importedMap);

		assertEquals(1, baseMap.getGroup().size());
		assertEquals("GroupA", baseMap.getGroup().get(0).getName());
	}

	@Test
	void mergeStructureMaps_shouldInheritDescriptionIfBaseEmpty() {
		importedMap.setDescription("Imported desc");

		mapper.mergeStructureMaps(baseMap, importedMap);

		assertEquals("Imported desc", baseMap.getDescription());
	}

	@Test
	void mergeStructureMaps_shouldNotDuplicateExistingGroup() {
		StructureMap.StructureMapGroupComponent baseGroup = new StructureMap.StructureMapGroupComponent();
		baseGroup.setName("GroupA");
		baseMap.getGroup().add(baseGroup);

		StructureMap.StructureMapGroupComponent importedGroup = new StructureMap.StructureMapGroupComponent();
		importedGroup.setName("GroupA");
		importedMap.getGroup().add(importedGroup);

		mapper.mergeStructureMaps(baseMap, importedMap);

		assertEquals(1, baseMap.getGroup().size());
	}

	@Test
	void sameGroupSignature_shouldReturnTrueForMatchingGroups() {
		StructureMap.StructureMapGroupComponent g1 = new StructureMap.StructureMapGroupComponent();
		StructureMap.StructureMapGroupComponent g2 = new StructureMap.StructureMapGroupComponent();

		g1.setName("group");
		g2.setName("group");

		StructureMap.StructureMapGroupInputComponent input1 = new StructureMap.StructureMapGroupInputComponent();
		input1.setType("Patient");
		StructureMap.StructureMapGroupInputComponent input2 = input1.copy();

		g1.addInput(input1);
		g2.addInput(input2);

		assertTrue(mapper.sameGroupSignature(g1, g2));
	}

	@Test
	void sameGroupSignature_shouldReturnFalseForDifferentNames() {
		StructureMap.StructureMapGroupComponent g1 = new StructureMap.StructureMapGroupComponent();
		StructureMap.StructureMapGroupComponent g2 = new StructureMap.StructureMapGroupComponent();

		g1.setName("groupA");
		g2.setName("groupB");

		assertFalse(mapper.sameGroupSignature(g1, g2));
	}

	@Test
	void mergeGroups_shouldMergeExistingAndAddNewRules() throws Exception {
		// GIVEN
		StructureMap.StructureMapGroupComponent baseGroup = new StructureMap.StructureMapGroupComponent();
		StructureMap.StructureMapGroupRuleComponent baseRule = new StructureMap.StructureMapGroupRuleComponent();
		baseRule.setName("RuleA");
		baseGroup.addRule(baseRule);

		StructureMap.StructureMapGroupComponent importedGroup = new StructureMap.StructureMapGroupComponent();
		StructureMap.StructureMapGroupRuleComponent importedRule1 = new StructureMap.StructureMapGroupRuleComponent();
		importedRule1.setName("RuleA"); // même nom → fusion
		StructureMap.StructureMapGroupRuleComponent importedRule2 = new StructureMap.StructureMapGroupRuleComponent();
		importedRule2.setName("RuleB"); // nouveau → ajouté
		importedGroup.addRule(importedRule1);
		importedGroup.addRule(importedRule2);

		Mapper mapper = spy(new Mapper(null, null, null, null, null));
		doNothing().when(mapper).mergeRules(any(), any()); // évite la récursion

		// WHEN
		Method mergeGroups = Mapper.class.getDeclaredMethod(
			"mergeGroups",
			StructureMap.StructureMapGroupComponent.class,
			StructureMap.StructureMapGroupComponent.class
		);
		mergeGroups.setAccessible(true);
		mergeGroups.invoke(mapper, baseGroup, importedGroup);

		// THEN
		assertEquals(2, baseGroup.getRule().size());
		assertEquals("RuleB", baseGroup.getRule().get(0).getName(), "La nouvelle règle importée doit être ajoutée en tête");
		assertEquals("RuleA", baseGroup.getRule().get(1).getName(), "La règle existante reste après");
		verify(mapper).mergeRules(any(), any());
	}


	@Test
	void mergeRules_shouldMergeNestedRulesAndDependents() {
		StructureMap.StructureMapGroupRuleComponent baseRule = new StructureMap.StructureMapGroupRuleComponent();
		baseRule.setName("RuleA");

		StructureMap.StructureMapGroupRuleComponent importedRule = new StructureMap.StructureMapGroupRuleComponent();
		importedRule.setName("RuleA");

		StructureMap.StructureMapGroupRuleComponent subRule = new StructureMap.StructureMapGroupRuleComponent();
		subRule.setName("SubRule1");
		importedRule.addRule(subRule);

		StructureMap.StructureMapGroupRuleDependentComponent dependent = new StructureMap.StructureMapGroupRuleDependentComponent();
		dependent.setName("Dep1");
		importedRule.addDependent(dependent);

		mapper.mergeRules(baseRule, importedRule);

		assertEquals(1, baseRule.getRule().size());
		assertEquals("SubRule1", baseRule.getRule().get(0).getName());
		assertEquals(1, baseRule.getDependent().size());
	}

	@Test
	void mergeVariables_shouldAddNonExistingContainedResources() {
		Resource imported = new Observation();
		imported.setId("obs1");

		List<Resource> baseContained = new ArrayList<>();
		List<Resource> importedContained = List.of(imported);

		mapper.mergeVariables(baseContained, importedContained);

		assertEquals(1, baseContained.size());
	}

	@Test
	void mergeVariables_shouldSkipExistingResources() {
		Resource existing = new Observation();
		existing.setId("obs1");

		Resource imported = new Observation();
		imported.setId("obs1");

		List<Resource> baseContained = new ArrayList<>(List.of(existing));
		List<Resource> importedContained = List.of(imported);

		mapper.mergeVariables(baseContained, importedContained);

		assertEquals(1, baseContained.size());
	}
}