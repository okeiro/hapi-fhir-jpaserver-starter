package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.starter.mapping.model.*;
import ca.uhn.fhir.jpa.starter.mapping.model.Variable;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.*;
import ca.uhn.hl7v2.model.Group;
import ca.uhn.hl7v2.parser.ModelClassFactory;
import org.apache.commons.csv.CSVRecord;
import org.hl7.fhir.exceptions.DefinitionException;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.context.IWorkerContext;
import org.hl7.fhir.r4.fhirpath.ExpressionNode;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.utils.StructureMapUtilities;
import org.hl7.fhir.utilities.TerminologyServiceOptions;
import org.hl7.fhir.utilities.Utilities;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static ca.uhn.fhir.context.FhirVersionEnum.R4;
import static ca.uhn.fhir.jpa.starter.mapping.model.Variable.VariableMode.*;
import static ca.uhn.fhir.jpa.starter.mapping.service.CSVDataReader.parseCSVData;
import static ca.uhn.fhir.jpa.starter.mapping.service.JSONDataReader.parseJsonData;
import static ca.uhn.fhir.jpa.starter.mapping.service.XMLDataReader.parseXMLData;

/*
  Copyright (c) 2011+, HL7, Inc.
  All rights reserved.

  Redistribution and use in source and binary forms, with or without modification,
  are permitted provided that the following conditions are met:

   * Redistributions of source code must retain the above copyright notice, this
     list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above copyright notice,
     this list of conditions and the following disclaimer in the documentation
     and/or other materials provided with the distribution.
   * Neither the name of HL7 nor the names of its contributors may be used to
     endorse or promote products derived from this software without specific
     prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
  POSSIBILITY OF SUCH DAMAGE.

 */

/**
 * Based on {@see StructureMapUtilities}
 */
//This is supporting only FHIR R4
public class Mapper {

	public static final String MAP_EXPRESSION = "map.transform.expression";
	public static final String MAP_WHERE_EXPRESSION = "map.where.expression";
	public static final String MAP_WHERE_CHECK = "map.where.check";
	public static final String CSV_MIME_TYPE = "text/csv";
	public static final String JSON_MIME_TYPE = "application/json";
	public static final String HL7v2_MIME_TYPE = "text/x-hl7-ft";
	public static final String XML_MIME_TYPE = "application/xml";
	private static final Logger logger = LoggerFactory.getLogger(Mapper.class);
	private final IWorkerContext worker;
	private final FHIRPathEngine fhirPathEngine;
	private final StructureMapUtilities.ITransformerServices services;

	public Mapper(IWorkerContext worker, FHIRPathEngine fhirPathEngine,
					  StructureMapUtilities.ITransformerServices services) {
		this.worker = worker;
		this.fhirPathEngine = fhirPathEngine;
		this.services = services;
	}

	public Parameters map(StructureMap structureMap, Parameters parameters) {
		logger.info("Start Mapping using map : " + structureMap.getUrl());
		logger.warn("/!\\ Only one group from your StructureMap will be mapped /!\\");

		if (structureMap.getGroup().isEmpty()) {
			throw new InvalidRequestException("StructureMap.group is required !");
		}
		StructureMap.StructureMapGroupComponent group = structureMap.getGroup().get(0);

		Variables variables = new Variables();
		//Try to find inputs in Parameters resource
		for (StructureMap.StructureMapGroupInputComponent input : group.getInput()) {
			String inputName = input.getName();

			Binary parameter = (Binary) parameters.getParameter("input").getPart().stream()
				.filter(p -> inputName.equals(p.getName())).findFirst().orElse(new Parameters.ParametersParameterComponent())
				.getResource();

			// If no resource is found for a source input, then return an Exception
			if (parameter == null && StructureMap.StructureMapInputMode.SOURCE.equals(input.getMode())) {
				throw new InvalidRequestException(String.format("Missing input named '%s' in parameters !", inputName));
			} else if (parameter != null) {
				// If the Input is of type CSV, parse it (only for sources)
				if ("CSV".equals(input.getType())) {
					if (CSV_MIME_TYPE.equals(parameter.getContentType())) {
						variables.add(input.getMode().equals(StructureMap.StructureMapInputMode.SOURCE) ? INPUT : OUTPUT,
							inputName, new CSVRecords(parseCSVData(parameter.getContentAsBase64())));
					} else {
						throw new InvalidRequestException(String.format("Input named '%s' shall be a CSV (use mimetype '%s') !",
							inputName, CSV_MIME_TYPE));
					}
				} else if ("JSON".equals(input.getType())) {
					if (JSON_MIME_TYPE.equals(parameter.getContentType())) {
						variables.add(input.getMode().equals(StructureMap.StructureMapInputMode.SOURCE) ? INPUT : OUTPUT,
							inputName, parseJsonData(parameter.getContentAsBase64()));
					} else {
						throw new InvalidRequestException(String.format("Input named '%s' shall be a JSON (use mimetype '%s') !",
							inputName, JSON_MIME_TYPE));
					}
				} else if ("HL7v2".equals(input.getType())) {
					if (HL7v2_MIME_TYPE.equals(parameter.getContentType())) {
						variables.add(input.getMode().equals(StructureMap.StructureMapInputMode.SOURCE) ? INPUT : OUTPUT,
							inputName, HL7v2DataReader.parseData(parameter.getContentAsBase64()));
					} else {
						throw new InvalidRequestException(String.format("Input named '%s' shall be an HL7v2 Document (use mimetype '%s') !",
							inputName, HL7v2_MIME_TYPE));
					}
				} else if ("XML".equals(input.getType())) {
					if (XML_MIME_TYPE.equals(parameter.getContentType())) {
						variables.add(input.getMode().equals(StructureMap.StructureMapInputMode.SOURCE) ? INPUT : OUTPUT,
							inputName, parseXMLData(parameter.getContentAsBase64()));
					} else {
						throw new InvalidRequestException(String.format("Input named '%s' shall be a XML (use mimetype '%s') !",
							inputName, XML_MIME_TYPE));
					}

				}
				// Else try to parse as FHIR resource
				else {
					String stringContent = new String(Base64.getDecoder().decode(parameter.getContentAsBase64()), StandardCharsets.UTF_8);
					EncodingEnum encoding = EncodingEnum.detectEncoding(stringContent);
					IParser parser;

					switch (encoding) {
						case JSON:
							parser = FhirContext.forCached(R4).newJsonParser();
							break;
						case RDF:
							parser = FhirContext.forCached(R4).newRDFParser();
							break;
						case XML:
						default:
							parser = FhirContext.forCached(R4).newXmlParser();
							break;
					}

					try {
						IBaseResource resource = parser.parseResource(stringContent);
						variables.add(input.getMode().equals(StructureMap.StructureMapInputMode.SOURCE) ? INPUT : OUTPUT,
							inputName, resource);
					} catch (Exception e) {
						throw new InvalidRequestException(String.format("Could not parse resource for input named '%s' !", inputName));
					}
				}
			} else if (StructureMap.StructureMapInputMode.TARGET.equals(input.getMode())) {
				switch (input.getType()) {
					case "CSV":
						variables.add(OUTPUT, inputName, new CSVBuilder());
						break;
					case "JSON":
						variables.add(OUTPUT, inputName, new JSONBuilder());
						break;
					default:
						variables.add(OUTPUT, inputName, ResourceFactory.createResourceOrType(input.getType()));
						break;
				}
			}
		}

		executeGroup(new MappingContext(structureMap, group, variables), true);

		Parameters result = new Parameters();
		variables.getOutputs().stream().forEach(v -> {
			String type = group.getInput().stream().filter(i -> i.getName().equals(v.getName()))
				.map(StructureMap.StructureMapGroupInputComponent::getType)
				.findFirst().orElse("Resource");

			result.addParameter(new Parameters.ParametersParameterComponent()
				.setName(v.getName()).setResource(new Binary()
					.setContentType(getContentType(type))
					.setContentAsBase64(Base64.getEncoder()
						.encodeToString(
							v.getObject() instanceof IBaseResource
								? FhirContext.forCached(R4).newJsonParser().encodeResourceToString((IBaseResource) v.getObject()).getBytes(StandardCharsets.UTF_8)
								: v.getObject().toString().getBytes(StandardCharsets.UTF_8)))));
		});
		return result;
	}

	/**
	 * Get Content Type depending on the input type
	 * @param type the input type
	 * @return the content type value
	 */
	private String getContentType(String type) {
		switch (type) {
			case "CSV":
				return CSV_MIME_TYPE;
			case "JSON":
				return JSON_MIME_TYPE;
			case "XML":
				return XML_MIME_TYPE;
			case "HL7v2":
				return HL7v2_MIME_TYPE;
			default:
				return "application/fhir+json";
		}
	}

	/**
	 * Execute a group from a {@link MappingContext}.
	 *
	 * @param context the Mapping context.
	 * @param atRoot  true if this is a root group.
	 */
	private void executeGroup(MappingContext context, boolean atRoot) {
		//Execute group that are extended first. (recursive call)
		if (context.getGroup().hasExtends()) {
			ResolvedGroup resolvedGroup = resolveGroupReference(context, context.getGroup().getExtends());
			MappingContext childContext = new MappingContext(resolvedGroup.targetMap, resolvedGroup.target, context.getVariables().copy());
			executeGroup(childContext, false);
		}

		// Then execute rules from the group.
		for (StructureMap.StructureMapGroupRuleComponent rule : context.getGroup().getRule()) {
			context.setRule(rule);
			executeRule(context, atRoot);
		}
	}

	/**
	 * Execute a rule from the ConceptMap.
	 *
	 * @param context the Mapping context.
	 * @param atRoot  true if part of a root group.
	 */
	private void executeRule(MappingContext context, boolean atRoot) {
		Variables sourceVariables = context.getVariables().copy();

		List<Variables> sources;
		if (!context.getRule().getSource().isEmpty()) {
			context.setSources(context.getRule().getSource());

			sources = processSources(context, sourceVariables);

			// For each mapped sources, process the target.
			for (Variables variables : sources) {
				processTargets(new MappingContext(context.setVariables(variables)), atRoot);

				// If rule has sub-rules, execute them
				if (context.getRule().hasRule()) {
					for (StructureMap.StructureMapGroupRuleComponent childRule : context.getRule().getRule()) {
						MappingContext childContext = new MappingContext(context.getStructureMap(), context.getGroup(), variables);
						childContext.setRule(childRule);
						executeRule(childContext, false);
					}
				}
				// If no sub-rules, try to get dependent rules.
				else if (context.getRule().hasDependent()) {
					for (StructureMap.StructureMapGroupRuleDependentComponent dependent : context.getRule().getDependent()) {
						MappingContext childContext = new MappingContext(context.getStructureMap(), context.getGroup(), variables);
						executeDependency(childContext, dependent);
					}
				}
				else if (context.getRule().getSource().size() == 1 && context.getRule().getSourceFirstRep().hasVariable()
					&& context.getRule().getTarget().size() == 1 && context.getRule().getTargetFirstRep().hasVariable()
					&& context.getRule().getTargetFirstRep().getTransform() == StructureMap.StructureMapTransform.CREATE
					&& !context.getRule().getTargetFirstRep().hasParameter()) {

					Base source = (Base) variables.get(INPUT, context.getRule().getSourceFirstRep().getVariable());
					Base target = (Base) variables.get(OUTPUT, context.getRule().getTargetFirstRep().getVariable());
					String sourceType = source.fhirType();
					String targetType = target.fhirType();
					ResolvedGroup defGroup = resolveGroupByTypes(context, sourceType, targetType);
					Variables vdef = new Variables();
					vdef.add(INPUT, defGroup.target.getInput().get(0).getName(), source);
					vdef.add(OUTPUT, defGroup.target.getInput().get(1).getName(), target);
					MappingContext childContext = new MappingContext(defGroup.targetMap, defGroup.target, vdef);

					executeGroup(childContext, false);
				}
			}
		}
		//If no sources still process target
		else {
			processTargets(context, atRoot);

			// If rule has sub-rules, execute them
			if (context.getRule().hasRule()) {
				for (StructureMap.StructureMapGroupRuleComponent childRule : context.getRule().getRule()) {
					MappingContext childContext = new MappingContext(context.getStructureMap(), context.getGroup(), context.getVariables());
					childContext.setRule(childRule);
					executeRule(childContext, false);
				}
			}
			// If no sub-rules, try to get dependent rules.
			else if (context.getRule().hasDependent()) {
				for (StructureMap.StructureMapGroupRuleDependentComponent dependent : context.getRule().getDependent()) {
					MappingContext childContext = new MappingContext(context.getStructureMap(), context.getGroup(), context.getVariables());
					executeDependency(childContext, dependent);
				}
			}
			else if (context.getRule().getSource().size() == 1 && context.getRule().getSourceFirstRep().hasVariable()
				&& context.getRule().getTarget().size() == 1 && context.getRule().getTargetFirstRep().hasVariable()
				&& context.getRule().getTargetFirstRep().getTransform() == StructureMap.StructureMapTransform.CREATE
				&& !context.getRule().getTargetFirstRep().hasParameter()) {

				Base source = (Base) context.getVariables().get(INPUT, context.getRule().getSourceFirstRep().getVariable());
				Base target = (Base) context.getVariables().get(OUTPUT, context.getRule().getTargetFirstRep().getVariable());
				String sourceType = source.fhirType();
				String targetType = target.fhirType();
				ResolvedGroup defGroup = resolveGroupByTypes(context, sourceType, targetType);
				Variables vdef = new Variables();
				vdef.add(INPUT, defGroup.target.getInput().get(0).getName(), source);
				vdef.add(OUTPUT, defGroup.target.getInput().get(1).getName(), target);
				MappingContext childContext = new MappingContext(defGroup.targetMap, defGroup.target, vdef);

				executeGroup(childContext, false);
			}
		}
	}

	private List<Variables> processSources(MappingContext context, Variables localVariables) {
		String sourceContext = context.getSources().get(0).getContext();

		String type = context.getGroup().getInput().stream()
			.filter(input -> sourceContext.equals(input.getName()))
			.map(StructureMap.StructureMapGroupInputComponent::getType)
			.findFirst().orElse("Resource");
		//.orElse(context.getGroup().getInput().get(0).getType());

		switch (type) {
			case "CSV":
				return processCSVSource(context, localVariables);
			case "HL7v2":
				return processHL7v2Source(context, localVariables);
			case "XML":
				//Uses JSON processing as XML are translated to JSON
			case "JSON":
				return processJsonSource(context, localVariables);
			default:
				if ((localVariables.get(INPUT, context.getRule().getSource().get(0).getContext())) instanceof JSONObject) {
					return processJsonSource(context, localVariables);
				}
				return processFHIRSources(context, localVariables);
		}
	}

	/**
	 * Process CSV sources for a specific context.
	 *
	 * @param context        the Mapping context.
	 * @param localVariables local variables
	 * @return a list of variables for all matched sources
	 */
	private List<Variables> processCSVSource(MappingContext context, Variables localVariables) {
		if (context.getSources().stream().map(StructureMap.StructureMapGroupRuleSourceComponent::getContext)
			.distinct().count() > 1) {
			throw new InvalidRequestException(String.format("Rule \"%s\": multiple sources shall come from the same CSV input", context.getRule().getName()));
		}

		Object sourceObject = localVariables.get(INPUT, context.getSources().get(0).getContext());

		if (sourceObject == null) {
			throw new InvalidRequestException(
				String.format("Unknown input variable %s in %s for rule %s",
					context.getSources().get(0).getContext(),
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		} else if (!(sourceObject instanceof CSVRecord || sourceObject instanceof CSVRecords)) {
			throw new InvalidRequestException(
				String.format("Input variable %s in %s for rule %s is not a CSV Object !",
					context.getSources().get(0).getContext(),
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		} else if (context.getSources().stream().anyMatch(s -> !s.hasElement())) {
			throw new InvalidRequestException(
				String.format("Element should not be null in %s for rule %s !",
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		} else if (context.getSources().stream().anyMatch(s -> !s.hasType())) {
			throw new InvalidRequestException(
				String.format("Type should not be null in %s for rule %s !",
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		}

		List<List<Base>> items;

		CSVRecords csvRecords = (CSVRecords) sourceObject;

		items = csvRecords.getRecords().stream()
			.map(record -> processCSVRecord(context, record))
			.collect(Collectors.toList());

		List<Variables> result = new ArrayList<>();
		for (List<Base> recordItems : items) {
			Variables v = localVariables.copy();
			for (int i = 0; i < context.getSources().size(); i++) {
				if (context.getSources().get(i).hasVariable()) {
					v.add(INPUT, context.getSources().get(i).getVariable(), recordItems.get(i));
				}
			}
			result.add(v);
		}
		return result;
	}

	/**
	 * Process Json sources for a specific context.
	 *
	 * @param context        the Mapping context.
	 * @param localVariables local variables
	 * @return a list of variables for all matched sources
	 */
	private List<Variables> processJsonSource(MappingContext context, Variables localVariables) {
		//TODO Handle multiple sources ?
		if (context.getRule().getSource().size() > 1) {
			throw new InvalidRequestException(String.format("Rule \"%s\": multiple sources are not handled yet", context.getRule().getName()));
		}

		StructureMap.StructureMapGroupRuleSourceComponent source = context.getRule().getSource().get(0);

		Object sourceObject = localVariables.get(INPUT, context.getSources().get(0).getContext());

		if (sourceObject == null) {
			throw new InvalidRequestException(
				String.format("Unknown input variable %s in %s for rule %s",
					context.getSources().get(0).getContext(),
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		} else if (context.getSources().stream().anyMatch(s -> !s.hasElement())) {
			throw new InvalidRequestException(
				String.format("Element should not be null in %s for rule %s !",
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		} else if (context.getSources().stream().anyMatch(s -> !s.hasType())) {
			throw new InvalidRequestException(
				String.format("Type should not be null in %s for rule %s !",
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		}

		JSONObject jsonObject = (JSONObject) sourceObject;

		List<Object> items = processJsonObject(context, jsonObject);

		if (source.hasListMode() && !items.isEmpty()) {
			switch (source.getListMode()) {
				case FIRST:
					items = List.of(items.get(0));
					break;
				case NOTFIRST:
					if (!items.isEmpty()) {
						items.remove(0);
					}
					break;
				case LAST:
					items = List.of(items.get(items.size() - 1));
					break;
				case NOTLAST:
					if (!items.isEmpty()) {
						items.remove(items.size() - 1);
					}
					break;
				case ONLYONE:
					if (items.size() > 1) {
						throw new FHIRException("Rule \"" + context.getRule().getName() + "\": Check condition failed: the collection has more than one item");
					}
					break;
				case NULL:
			}
		}

		List<Variables> result = new ArrayList<>();
		if (source.hasVariable()) {
			for (Object base : items) {
				Variables variables = localVariables.copy();
				variables.add(INPUT, source.getVariable(), base);
				result.add(variables);
			}
		}
		return result;
	}

	/**
	 * Process HL7v2 sources for a specific context.
	 *
	 * @param context        the Mapping context.
	 * @param localVariables local variables
	 * @return a list of variables for all matched sources
	 */
	private List<Variables> processHL7v2Source(MappingContext context, Variables localVariables) {
		//TODO Handle multiple sources ?
		if (context.getRule().getSource().size() > 1) {
			throw new InvalidRequestException(String.format("Rule \"%s\": multiple sources are not handled yet", context.getRule().getName()));
		}

		StructureMap.StructureMapGroupRuleSourceComponent source = context.getRule().getSource().get(0);

		Object sourceObject = localVariables.get(INPUT, context.getSources().get(0).getContext());

		if (sourceObject == null) {
			throw new InvalidRequestException(
				String.format("Unknown input variable %s in %s for rule %s",
					context.getSources().get(0).getContext(),
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		} else if (context.getSources().stream().anyMatch(s -> !s.hasElement())) {
			throw new InvalidRequestException(
				String.format("Element should not be null in %s for rule %s !",
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		} else if (context.getSources().stream().anyMatch(s -> !s.hasType())) {
			throw new InvalidRequestException(
				String.format("Type should not be null in %s for rule %s !",
					context.getStructureMap().getUrl(),
					context.getRule().getName()));
		}

		List<Object> items;

		//Only message or can it be segments ?
		if (sourceObject instanceof Message) {
			items = processHL7v2Object(context, (Message) sourceObject);
		} else if (sourceObject instanceof GenericSegment) {
			items = processHL7v2Object(context, (GenericSegment) sourceObject);
		} else {
			items = new ArrayList<>();
		}

		List<Variables> result = new ArrayList<>();
		if (source.hasVariable()) {
			for (Object base : items) {
				Variables variables = localVariables.copy();
				variables.add(INPUT, source.getVariable(), base);
				result.add(variables);
			}
		}
		return result;
	}

	private List<Variables> processFHIRSources(MappingContext context, Variables localVariables) {
		//TODO Handle multiple sources ?
		if (context.getRule().getSource().size() > 1) {
			throw new InvalidRequestException(String.format("Rule \"%s\": multiple sources are not handled yet", context.getRule().getName()));
		}

		StructureMap.StructureMapGroupRuleSourceComponent source = context.getRule().getSource().get(0);

		List<Base> items;
		if (source.getContext().equals("@search")) {
			ExpressionNode expressionNode = (ExpressionNode) source.getUserData("map.search.expression");
			if (expressionNode == null) {
				expressionNode = this.fhirPathEngine.parse(source.getElement());
				source.setUserData("map.search.expression", expressionNode);
			}

			String search = this.fhirPathEngine.evaluateToString(localVariables, null, null, new StringType(), expressionNode);
			items = this.services.performSearch(context.getAppInfo(), search);
		} else {
			items = new ArrayList<>();
			Base base = (Base) localVariables.get(INPUT, source.getContext());
			if (base == null) {
				base = (Base) localVariables.get(OUTPUT, source.getContext());
			}
			if (base == null && !source.hasDefaultValue()) {
				throw new FHIRException(String.format("Unknown input variable %s in rule %s and no default value is provided ! ",
					source.getContext(), context.getRule().getName()));
			} else if (base != null) {
				if (!source.hasElement()) {
					items.add(base);
				} else {
					for (Base b : base.listChildrenByName(source.getElement(), true)) {
						if (b != null) {
							items.add(b);
						}
					}
					if (items.isEmpty() && source.hasDefaultValue()) {
						items.add(source.getDefaultValue());
					}
				}
			} else if (source.hasDefaultValue()) {
				items.add(source.getDefaultValue());
			}
		}

		if (source.hasType()) {
			items = items.stream()
				.filter(item -> item != null && source.getType().equals(item.fhirType()))
				.collect(Collectors.toList());
		}

		if (source.hasCondition()) {
			ExpressionNode expression = (ExpressionNode) source.getUserData(MAP_WHERE_EXPRESSION);
			if (expression == null) {
				expression = fhirPathEngine.parse(source.getCondition());
				source.setUserData(MAP_WHERE_EXPRESSION, expression);
			}

			ExpressionNode finalExpression = expression;
			items = items.stream()
				.filter(item -> fhirPathEngine.evaluateToBoolean(localVariables, null, null, item, finalExpression))
				.collect(Collectors.toList());
		}

		if (source.hasCheck()) {
			ExpressionNode expression = (ExpressionNode) source.getUserData(MAP_WHERE_CHECK);
			if (expression == null) {
				expression = fhirPathEngine.parse(source.getCheck());
				source.setUserData(MAP_WHERE_CHECK, expression);
			}
			for (Base item : items) {
				if (!fhirPathEngine.evaluateToBoolean(localVariables, null, null, item, expression)) {
					throw new FHIRException("Rule \"" + context.getRule().getName() + "\": Check condition failed");
				}
			}
		}

		if (source.hasListMode() && !items.isEmpty()) {
			switch (source.getListMode()) {
				case FIRST:
					items = List.of(items.get(0));
					break;
				case NOTFIRST:
					if (!items.isEmpty()) {
						items.remove(0);
					}
					break;
				case LAST:
					items = List.of(items.get(items.size() - 1));
					break;
				case NOTLAST:
					if (!items.isEmpty()) {
						items.remove(items.size() - 1);
					}
					break;
				case ONLYONE:
					if (items.size() > 1) {
						throw new FHIRException("Rule \"" + context.getRule().getName() + "\": Check condition failed: the collection has more than one item");
					}
					break;
				case NULL:
			}
		}
		List<Variables> result = new ArrayList<>();
		if (source.hasVariable()) {
			for (Base base : items) {
				Variables variables = localVariables.copy();
				variables.add(INPUT, source.getVariable(), base);
				result.add(variables);
			}
		}
		return result;
	}

	private List<Base> processCSVRecord(MappingContext context, CSVRecord sourceRecord) {

		List<Base> items = new ArrayList<>();
		boolean skip = false;

		for (StructureMap.StructureMapGroupRuleSourceComponent source : context.getSources()) {
			Base item = null;
			String elementName = source.getElement();
			//TODO Catch errors ?
			String elementStringValue = sourceRecord.get(elementName);
			if (elementStringValue != null) {
				item = getFHIRItem(elementStringValue, source.getType());
			} else if (source.hasDefaultValue() && source.getType().equals(source.getDefaultValue().fhirType())) {
				item = source.getDefaultValue();
			} else if (!source.getType().equals(source.getDefaultValue().fhirType())) {
				throw new InvalidRequestException(
					String.format("Default value type does not match in %s for rule %s source %s !",
						context.getStructureMap().getUrl(), context.getRule().getName(), source.getContext()));
			}

			checkValues(source, List.of(item), context.getRule().getName());
			if (!matchesCondition(source, item, context.getVariables())) {
				skip = true;
				break;
			}

			items.add(item);
		}
		return skip ? null : items;
	}

	/**
	 * Processes a JSON object according to the mapping context.
	 *
	 * @param context    the Mapping context.
	 * @param jsonObject The JSON object to process.
	 * @return A list of Base items resulting from the processing, or null if the processing is skipped.
	 * @throws InvalidRequestException If there's an issue with the mapping rules or the structure of the JSON data.
	 */
	private List<Object> processJsonObject(MappingContext context, JSONObject jsonObject) {
		List<Object> items = new ArrayList<>();
		boolean skip = false;

		for (StructureMap.StructureMapGroupRuleSourceComponent source : context.getSources()) {
			Base item = null;
			String elementName = source.getElement();

			try {
				Object element = jsonObject.get(elementName);
				if (element instanceof JSONObject) {
					items.add(element);
					if (source.hasCheck()) {
						throw new InvalidRequestException("Check not supported on JSON Object !");
					} else if (source.hasCondition()) {
						throw new InvalidRequestException("Condition not supported on JSON Object !");
					}
				} else if (element instanceof JSONArray) {
					for (int i = 0; i < ((JSONArray) element).length(); i++) {
						Object elementValue = ((JSONArray) element).get(i);

						if (elementValue instanceof JSONObject) {
							items.add(elementValue);
							if (source.hasCheck()) {
								throw new InvalidRequestException("Check not supported on JSON Object !");
							} else if (source.hasCondition()) {
								throw new InvalidRequestException("Condition not supported on JSON Object !");
							}
						} else {
							String elementStringValue = elementValue.toString();
							if (elementStringValue != null) {
								item = getFHIRItem(elementStringValue, source.getType());
							} else if (source.hasDefaultValue() && source.getType().equals(source.getDefaultValue().fhirType())) {
								item = source.getDefaultValue();
							} else if (!source.getType().equals(source.getDefaultValue().fhirType())) {
								throw new InvalidRequestException(
									String.format("Default value type does not match in %s for rule %s source %s !",
										context.getStructureMap().getUrl(), context.getRule().getName(), source.getContext()));
							}

							checkValues(source, List.of(item), context.getRule().getName());
							if (!matchesCondition(source, item, context.getVariables())) {
								skip = true;
								break;
							}

							items.add(item);
						}
					}
				} else {
					String elementStringValue = element.toString();
					if (elementStringValue != null) {
						item = getFHIRItem(elementStringValue, source.getType());
					} else if (source.hasDefaultValue() && source.getType().equals(source.getDefaultValue().fhirType())) {
						item = source.getDefaultValue();
					} else if (!source.getType().equals(source.getDefaultValue().fhirType())) {
						throw new InvalidRequestException(
							String.format("Default value type does not match in %s for rule %s source %s !",
								context.getStructureMap().getUrl(), context.getRule().getName(), source.getContext()));
					}

					checkValues(source, List.of(item), context.getRule().getName());
					if (!matchesCondition(source, item, context.getVariables())) {
						skip = true;
						break;
					}

					items.add(item);
				}
			} catch (JSONException e) {
				logger.info("Field not found in JSON source !", e);
			}
		}
		return skip ? new ArrayList<>() : items;
	}

	/**
	 * Processes a HL7v2 object according to the mapping context.
	 *
	 * @param context    the Mapping context.
	 * @param hl7v2Object The HL7v2 object to process.
	 * @return A list of Base items resulting from the processing, or null if the processing is skipped.
	 * @throws InvalidRequestException If there's an issue with the mapping rules or the structure of the HL7v2 data.
	 */
	List<Object> processHL7v2Object(MappingContext context, Object hl7v2Object) {
		List<Object> items = new ArrayList<>();
		boolean skip = false;

		for (StructureMap.StructureMapGroupRuleSourceComponent source : context.getSources()) {
			Base item = null;
			String elementName = source.getElement();

			try {
				Path path = new Path(elementName);

				List<GenericSegment> segments;
				if (hl7v2Object instanceof Message) {
					if (path.getFieldRepetition() != null) {
						int segmentRepetition = path.getSegmentRepetition() != null ? path.getSegmentRepetition() : 0;
						segments = List.of((GenericSegment) ((Message) hl7v2Object).get(path.getSegment(), segmentRepetition));
					} else {
						segments = Arrays.stream(((Message) hl7v2Object).getAll(path.getSegment()))
							.map(this::toGenericSegment)
							.collect(Collectors.toList());
					}
				} else {
					segments = List.of(toGenericSegment(hl7v2Object));
				}

				if (path.getField() == null) {
					items.addAll(segments);
				} else {
					List<Varies> fields;
					if (path.getFieldRepetition() != null) {
						fields = List.of((Varies) segments.get(0).getField(path.getField(), path.getFieldRepetition()));
					} else {
						fields = Arrays.stream(segments.get(0).getField(path.getField()))
							.map(t -> (Varies) t)
							.collect(Collectors.toList());
					}

					String elementStringValue = null;

					if (!fields.isEmpty()) {
						if (path.getComponent() != null) {
							GenericComposite fieldData = (GenericComposite) fields.get(0).getData();
							Varies component = (Varies) fieldData.getComponent(path.getComponent());
							if (path.getSubComponent() != null) {
								GenericComposite componentData = (GenericComposite) component.getData();
								Varies subComponent = (Varies) componentData.getComponent(path.getSubComponent());
								elementStringValue = subComponent.getData().toString();
							} else {
								elementStringValue = component.getData().toString();
							}
						} else {
							elementStringValue = fields.get(0).getData().toString();
						}
					}

					if (elementStringValue != null) {
						item = getFHIRItem(elementStringValue, source.getType());
					} else if (source.hasDefaultValue() && source.getType().equals(source.getDefaultValue().fhirType())) {
						item = source.getDefaultValue();
					} else if (source.hasDefaultValue() && !source.getType().equals(source.getDefaultValue().fhirType())) {
						throw new InvalidRequestException(
							String.format("Default value type does not match in %s for rule %s source %s !",
								context.getStructureMap().getUrl(), context.getRule().getName(), source.getContext()));
					}

					if (item != null) {
						checkValues(source, List.of(item), context.getRule().getName());
						if (!matchesCondition(source, item, context.getVariables())) {
							skip = true;
							break;
						}
						items.add(item);
					} else {
						logger.info("Field not found in HL7v2 source : " + source.getElement());
					}
				}
			} catch (ClassCastException | HL7Exception e) {
				logger.info("Field not found in HL7v2 source !", e);
			}
		}
		return skip ? new ArrayList<>() : items;
	}

	/**
	 * Converts an object (GenericSegment or typed Segment) to GenericSegment
	 */
	private GenericSegment toGenericSegment(Object obj) {
		if (obj instanceof GenericSegment) {
			return (GenericSegment) obj;
		}
		if (obj instanceof Segment) {
			Segment seg = (Segment) obj;
			try {
				String encoded = seg.encode();
				GenericSegment gen = new GenericSegment(seg.getParent(), seg.getName());
				gen.parse(encoded);
				return gen;
			} catch (HL7Exception e) {
				logger.info("Failed to convert segment " + seg.getName() + " to GenericSegment", e);
			}
		}
		throw new IllegalArgumentException("Unsupported HL7v2 object type: " + obj.getClass());
	}


	/**
	 * Get a FHIR Item from a CSV extracted string.
	 * Supported types are those of {@link QuestionnaireResponse} responses values :
	 * string, boolean, decimal, integer, date, dateTime, time, uri, Attachment, Coding, Quantity, Reference.
	 * <p>
	 * Attachment, Coding, Quantity and Reference are not yes supported.
	 * Default value is string.
	 *
	 * @param elementStringValue the extracted String.
	 * @param elementType        the element Type for casting.
	 * @return the FHIR element (see {@link Base}
	 */
	private Base getFHIRItem(String elementStringValue, String elementType) {
		switch (elementType) {
			case "boolean":
				return new BooleanType(elementStringValue);
			case "decimal":
				return new DecimalType(elementStringValue);
			case "integer":
				return new IntegerType(elementStringValue);
			case "date":
				return new DateType(elementStringValue);
			case "dateTime":
				return new DateTimeType(elementStringValue);
			case "time":
				return new TimeType(elementStringValue);
			case "uri":
				return new UriType(elementStringValue);
			case "code":
				return new CodeType(elementStringValue);
			case "Attachment":
			case "Coding":
			case "Quantity":
			case "Reference":
				throw new UnsupportedOperationException(String.format("Source element type not supported : %s", elementType));
			case "string":
			default:
				return new StringType(elementStringValue);
		}
	}

	/**
	 * Checks if a single value matches the source Condition. True is returned if no condition found in source.
	 *
	 * @param source the source.
	 * @param item   the item.
	 * @return true if matches.
	 */
	private boolean matchesCondition(StructureMap.StructureMapGroupRuleSourceComponent source, Base item, Variables variables) {
		if (source.hasCondition()) {
			ExpressionNode expression = fhirPathEngine.parse(source.getCondition());
			return fhirPathEngine.evaluateToBoolean(variables, null, null, item, expression);
		}
		return true;
	}

	/**
	 * If source has a check, verify that no item matches the check. Else, throw an Exception.
	 *
	 * @param source the Source element.
	 * @param items  the items to check.
	 * @param ruleId the Rule ID.
	 */
	private void checkValues(StructureMap.StructureMapGroupRuleSourceComponent source, List<Base> items, String ruleId) {
		if (source.hasCheck()) {
			ExpressionNode expr = fhirPathEngine.parse(source.getCheck());
			for (Base item : items) {
				if (!fhirPathEngine.evaluateToBoolean(null, null, null, item, expr)) {
					throw new FHIRException(String.format("Rule \"%s\": Check condition failed", ruleId));
				}
			}
		}
	}

	/**
	 * Process targets from a specific Mapping Context.
	 *
	 * @param context the Mapping Context.
	 * @param atRoot  true if in a root group.
	 */
	private void processTargets(MappingContext context, boolean atRoot) {
		for (StructureMap.StructureMapGroupRuleTargetComponent target : context.getRule().getTarget()) {
			context.setTarget(target);
			processTarget(context, atRoot);
		}
	}

	/**
	 * Process a single target from a specific Mapping Context.
	 *
	 * @param context the Mapping Context.
	 * @param atRoot  true if in a root group.
	 */
	private void processTarget(MappingContext context, boolean atRoot) {
		Object targetedElement = null;
		if (context.getTarget().hasContext()) {
			targetedElement = context.getVariables().get(OUTPUT, context.getTarget().getContext());
			if(targetedElement == null) {
				targetedElement = context.getVariables().get(INPUT, context.getTarget().getContext());
			}
			if (targetedElement == null || !context.getTarget().hasElement()) {
				throw new InvalidRequestException(String.format("Rule \"%s\": target context not known: %s",
					context.getRule().getName(), context.getTarget().getContext()));
			}
		}

		Base result = null;

		if (targetedElement instanceof Base) {
			//If there is a transform, transform the element
			if (context.getTarget().hasTransform()) {
				result = runTransform(context, (Base) targetedElement, atRoot);
				if (result != null && targetedElement != null) {
					result = setProperty(context, (Base) targetedElement, result);
				}
			}
			// If there is no transform, use it as is.
			else if (targetedElement != null) {
				// If the listMode is share, share the variable.
				if (context.getTarget().hasListMode(StructureMap.StructureMapTargetListMode.SHARE)) {
					result = (Base) context.getVariables().get(SHARED, context.getTarget().getListRuleId());
					if (result == null) {
						result = ((Base) targetedElement).makeProperty(context.getTarget().getElement().hashCode(), context.getTarget().getElement());
						context.getVariables().add(SHARED, context.getTarget().getListRuleId(), result);
					}
				}
				// Else just create the element.
				else {
					result = ((Base) targetedElement).makeProperty(context.getTarget().getElement().hashCode(), context.getTarget().getElement());
				}
			}
		} else if (targetedElement instanceof CSVBuilder) {
			String element = context.getTarget().getElement();

			if (context.getTarget().hasTransform()) {
				result = runTransform(context, null, atRoot);
			}

			if ("header".equals(element) && result != null) {
				//((CSVBuilder) targetedElement).addHeader(result.castToString(result).getValue());
			} else {
				Base elementVariable = (Base) context.getVariables().get(INPUT, element);
				String elementName = elementVariable.castToString(elementVariable).getValue();

				((CSVBuilder) targetedElement).addValue(elementName,
					result != null ? result.castToString(result).getValue() : "" );
			}
		} else if (targetedElement instanceof JSONBuilder) {
			String element = context.getTarget().getElement();

			if (context.getTarget().hasTransform()) {
				result = runTransform(context, null, atRoot);
			}

			if  (result != null) {
				((JSONBuilder) targetedElement).putByPath(element, result.castToString(result).getValue());
			}
		}

		// If the target defines a variable, put the result in it.
		if (context.getTarget().hasVariable() && result != null) {
			context.getVariables().add(OUTPUT, context.getTarget().getVariable(), result);
		}
	}

	/**
	 * Transform elements according to the context.
	 *
	 * @param context         the Mapping Context.
	 * @param targetedElement the Target elements.
	 * @param root            true if root group.
	 * @return the transformed element.
	 */
	private Base runTransform(MappingContext context, Base targetedElement, boolean root) {
		try {
			switch (context.getTarget().getTransform()) {
				case CREATE:
					return createTransform(context, targetedElement, root);
				case COPY:
					// Copy only uses the first parameter
					return getParameterValue(context.getVariables(), context.getTarget().getParameter().get(0));
				case EVALUATE:
					return evaluateTransform(context);
				case TRUNCATE:
					String source = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(0), context.getTarget().toString());
					String len = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(1), context.getTarget().toString());
					if (Utilities.isInteger(len)) {
						int l = Integer.parseInt(len);
						if (source.length() > l)
							source = source.substring(0, l);
					}
					return new StringType(source);
				case CAST:
					if (context.getTarget().getParameter().size() == 1) {
						throw new FHIRException("Implicit type parameters on cast not yet supported");
					}
					String castSource = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(0), context.getTarget().toString());
					String castTarget = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(1), context.getTarget().toString());
					switch (castTarget) {
						case "string":
						case "text":
							return new StringType(castSource);
						case "id":
							return new IdType(castSource);
						case "integer":
							return new IntegerType(castSource);
						case "decimal":
							return new DecimalType(castSource);
						case "boolean":
							return new BooleanType(castSource);
						case "date":
							return new DateType(castSource);
						case "dateTime":
							return new DateTimeType(castSource);
						case "time":
							return new TimeType(castSource);
						default:
							throw new FHIRException(String.format("Cast to %s not yet supported", castTarget));
					}
				case APPEND:
					StringBuilder sb = new StringBuilder(getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(0), context.getTarget().toString()));
					for (int i = 1; i < context.getTarget().getParameter().size(); i++) {
						sb.append(getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(i), context.getTarget().toString()));
					}
					return new StringType(sb.toString());
				case TRANSLATE:
					return translate(context);
				case REFERENCE:
					Base parameterValue = getParameterValue(context.getVariables(), context.getTarget().getParameter().get(0));
					if (parameterValue == null) {
						throw new FHIRException(String.format("Rule \"%s\": Unable to find parameter %s",
							context.getRule().getName(), ((IdType) context.getTarget().getParameter().get(0).getValue()).asStringValue()));
					} else if (!parameterValue.isResource()) {
						throw new FHIRException(String.format("Rule \"%s\": Transform engine cannot point at an element of type %s",
							context.getRule().getName(), parameterValue.fhirType()));
					} else {
						String id = parameterValue.getIdBase();
						if (id == null) {
							id = UUID.randomUUID().toString().toLowerCase();
							parameterValue.setIdBase(id);
						}
						return new Reference().setReference(parameterValue.fhirType() + "/" + id);
					}
				case UUID:
					return new IdType(UUID.randomUUID().toString());
				case POINTER:
					parameterValue = getParameterValue(context.getVariables(), context.getTarget().getParameter().get(0));
					if (parameterValue instanceof Resource) {
						return new UriType("urn:uuid:" + ((Resource) parameterValue).getId());
					} else {
						throw new FHIRException(String.format("Rule \"%s\": Transform engine cannot point at an element of type %s",
							context.getRule().getName(), parameterValue.fhirType()));
					}
				case CC:
					return new CodeableConcept().addCoding(buildCoding(
						getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(0), context.getTarget().toString()),
						getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(1), context.getTarget().toString()),
						context.getTarget().getParameter().size() >= 3
							? getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(2), context.getTarget().toString())
							: null));
				case C:
					return buildCoding(getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(0), context.getTarget().toString()),
						getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(1), context.getTarget().toString()),
						context.getTarget().getParameter().size() >= 3
							? getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(2), context.getTarget().toString())
							: null);
				case DATEOP:
					String dateSource = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(0), context.getTarget().toString());
					String inputFormat = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(1), context.getTarget().toString());
					String outputFormatOrType = (context.getTarget().getParameter().size() > 2) ? getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(2), context.getTarget().toString()) : null;
					boolean forceInstant = outputFormatOrType != null && "instant".equalsIgnoreCase(outputFormatOrType.trim());

					DateTimeFormatter outputFormatter = (!forceInstant && outputFormatOrType != null)
						? DateTimeFormatter.ofPattern(outputFormatOrType)
						: null;

					DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern(inputFormat);

					try {
						LocalDateTime ldt = LocalDateTime.parse(dateSource, inputFormatter);
						if (forceInstant) {
							return new InstantType(Date.from(ldt.atZone(ZoneOffset.UTC).toInstant()));
						}
						if (outputFormatter != null) {
							String formatted = ldt.atZone(ZoneOffset.UTC).format(outputFormatter);
							return new DateTimeType(formatted);
						}
						return new DateTimeType(Date.from(ldt.atZone(ZoneOffset.UTC).toInstant()));

					} catch (Exception e1) {
						try {
							LocalDate ld = LocalDate.parse(dateSource, inputFormatter);
							if (forceInstant) {
								return new InstantType(Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant()));
							}
							if (outputFormatter != null) {
								String formatted = ld.atStartOfDay(ZoneOffset.UTC).format(outputFormatter);
								return new DateTimeType(formatted);
							}
							return new DateType(Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant()));

						} catch (Exception e2) {
							throw new IllegalArgumentException(
								String.format("Could not parse date '%s' with input format '%s'",
									dateSource, inputFormat), e2);
						}
					}
				case ESCAPE:
					throw new UnsupportedOperationException(String.format("Rule \"%s\": Transform %s not supported yet", context.getRule().getName(), context.getTarget().getTransform().toCode()));
				default:
					throw new UnsupportedOperationException(String.format("Rule \"%s\": Transform Unknown: %s", context.getRule().getName(), context.getTarget().getTransform().toCode()));
			}
		} catch (Exception e) {
			throw new FHIRException(String.format("Exception executing transform %s on Rule \"%s\": %s",
				context.getTarget().toString(), context.getRule().getName(), e.getMessage()), e);
		}
	}

	/**
	 * Create a new element for target element depending on the Mapping Context.
	 *
	 * @param context         the Mapping Context.
	 * @param targetedElement the targeted element.
	 * @param root            true if root group.
	 * @return the transformed element.
	 */
	private Base createTransform(MappingContext context, Base targetedElement, boolean root) {
		String typeName;
		// If there is no parameter for the transform
		if (context.getTarget().getParameter().isEmpty()) {
			String sourceVariable = context.getRule().getSource().size() == 1 ? context.getRule().getSourceFirstRep().getVariable() : null;
			String[] types = targetedElement.getTypesForProperty(context.getTarget().getElement().hashCode(), context.getTarget().getElement());
			if (types.length == 1 && !"*".equals(types[0]) && !types[0].equals("Resource")) {
				typeName = types[0];
			} else if (sourceVariable != null) {
				typeName = determineTypeFromSourceType(context, (Base) context.getVariables().get(INPUT, sourceVariable));
			} else {
				throw new Error("Cannot determine type implicitly because there is no single input variable");
			}
		}
		// Parameters are available for the transform
		else {
			// First Parameter needs to be the Type of the created resource/element
			typeName = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(0), context.getTarget().toString());
			// Try to resolve if the type is a link to a StructureDefinition (to get base FHIR Type)
			for (StructureMap.StructureMapStructureComponent structure : context.getStructureMap().getStructure()) {
				if (structure.getMode() == StructureMap.StructureMapModelMode.TARGET && structure.hasAlias() && typeName.equals(structure.getAlias())) {
					typeName = structure.getUrl();
					break;
				}
			}
		}

		Base result = services != null ? services.createType(context.getAppInfo(), typeName) : ResourceFactory.createResourceOrType(typeName);
		if (result.isResource() && !result.fhirType().equals("Parameters")) {
			if (services != null) {
				result = services.createResource(context.getAppInfo(), result, root);
			}
		}
		return result;
	}

	/**
	 * Determine FHIR Type from a source element.
	 *
	 * @param context the Mapping Context.
	 * @param base    the source element.
	 * @return the type of the element.
	 */
	private String determineTypeFromSourceType(MappingContext context, Base base) {
		String type = base.fhirType();
		String userType = "type^" + type;

		if (context.getGroup().hasUserData(userType)) {
			return context.getGroup().getUserString(userType);
		}

		StructureMap.StructureMapGroupComponent resolvedGroup = null;
		StructureMap targetMap = context.getStructureMap();
		for (StructureMap.StructureMapGroupComponent group : context.getStructureMap().getGroup()) {
			if (matchesByType(context.getStructureMap(), group, type)) {
				if (resolvedGroup == null) {
					resolvedGroup = group;
				} else {
					throw new InvalidRequestException(String.format("Multiple possible matches looking for default rule for '%s' in %s",
						type, context.getStructureMap().getUrl()));
				}
			}
		}
		if (resolvedGroup != null) {
			String result = getActualType(context.getStructureMap(), resolvedGroup.getInput().get(1).getType());
			context.getGroup().setUserData(userType, result);
			return result;
		}

		for (UriType importedMaps : context.getStructureMap().getImport()) {
			List<StructureMap> importedMapList = findMatchingMaps(importedMaps.getValue());
			if (importedMapList.isEmpty()) {
				throw new FHIRException(String.format("Unable to find map(s) for import : %s", importedMaps.getValue()));
			}
			for (StructureMap importedMap : importedMapList) {
				if (!importedMap.getUrl().equals(context.getStructureMap().getUrl())) {
					for (StructureMap.StructureMapGroupComponent group : importedMap.getGroup()) {
						if (matchesByType(importedMap, group, type)) {
							if (resolvedGroup == null) {

								resolvedGroup = group;
							} else {
								throw new FHIRException(String.format("Multiple possible matches for default rule for '%s' in %s (%s) and %s (%s)",
									type, targetMap.getUrl(), resolvedGroup.getName(), importedMap.getUrl(), group.getName()));

							}
						}
					}
				}
			}
		}
		if (resolvedGroup == null) {
			throw new FHIRException(String.format("No matches found for default rule for '%s' from %s", type, context.getStructureMap().getUrl()));
		}
		String result = getActualType(targetMap, resolvedGroup.getInput().get(1).getType());
		context.getGroup().setUserData(userType, result);
		return result;
	}

	/**
	 * Match a group to a specific type.
	 *
	 * @param map   the StructureMap.
	 * @param group the Group.
	 * @param type  the Type to match.
	 * @return true if matching.
	 */
	private boolean matchesByType(StructureMap map, StructureMap.StructureMapGroupComponent group, String type) {
		if (group.getTypeMode() != StructureMap.StructureMapGroupTypeMode.TYPEANDTYPES) {
			return false;
		} else if (group.getInput().size() != 2
			|| group.getInput().get(0).getMode() != StructureMap.StructureMapInputMode.SOURCE
			|| group.getInput().get(1).getMode() != StructureMap.StructureMapInputMode.TARGET) {
			return false;
		} else {
			return matchesType(map, type, group.getInput().get(0).getType());
		}
	}

	/**
	 * Match the group to a type.
	 *
	 * @param map        the StructureMap.
	 * @param group      the Group.
	 * @param sourceType the source type.
	 * @param targetType the target type.
	 * @return true if matches.
	 */
	private boolean matchesByType(StructureMap map, StructureMap.StructureMapGroupComponent group, String sourceType, String targetType) {
		if (group.getTypeMode() == StructureMap.StructureMapGroupTypeMode.NONE) {
			return false;
		} else if (group.getInput().size() != 2 || group.getInput().get(0).getMode() != StructureMap.StructureMapInputMode.SOURCE || group.getInput().get(1).getMode() != StructureMap.StructureMapInputMode.TARGET) {
			return false;
		} else if (!group.getInput().get(0).hasType() || !group.getInput().get(1).hasType()) {
			return false;
		} else {
			return matchesType(map, sourceType, group.getInput().get(0).getType()) && matchesType(map, targetType, group.getInput().get(1).getType());
		}
	}

	/**
	 * Matches two types.
	 *
	 * @param map        the StructureMap
	 * @param actualType the actual type found
	 * @param statedType the stated type.
	 * @return true if matched.
	 */
	//TODO Check structure from a remote server ?
	private boolean matchesType(StructureMap map, String actualType, String statedType) {
		// check the aliases
		for (StructureMap.StructureMapStructureComponent structure : map.getStructure()) {
			if (structure.hasAlias() && statedType.equals(structure.getAlias())) {
				StructureDefinition structureDefinition = worker.fetchResource(StructureDefinition.class, structure.getUrl());
				if (structureDefinition != null) {
					statedType = structureDefinition.getType();
				}
				break;
			}
		}
		if (Utilities.isAbsoluteUrl(actualType)) {
			StructureDefinition structureDefinition = worker.fetchResource(StructureDefinition.class, actualType);
			if (structureDefinition != null) {
				actualType = structureDefinition.getType();
			}
		}
		if (Utilities.isAbsoluteUrl(statedType)) {
			StructureDefinition structureDefinition = worker.fetchResource(StructureDefinition.class, statedType);
			if (structureDefinition != null) {
				statedType = structureDefinition.getType();
			}
		}
		return actualType.equals(statedType);
	}

	/**
	 * Get actual type from stated type.
	 *
	 * @param map        the StructureMap.
	 * @param statedType the stated type.
	 * @return the Actual type.
	 */
	private String getActualType(StructureMap map, String statedType) {
		// check the aliases
		for (StructureMap.StructureMapStructureComponent structure : map.getStructure()) {
			if (structure.hasAlias() && statedType.equals(structure.getAlias())) {
				StructureDefinition structureDefinition = worker.fetchResource(StructureDefinition.class, structure.getUrl());
				if (structureDefinition == null) {
					throw new ResourceNotFoundException(String.format("Unable to resolve structure %s", structure.getUrl()));
				}
				return structureDefinition.getType(); // was getId, see if not working ?
			}
		}
		return statedType;
	}

	/**
	 * Find maps from imports.
	 * Value can contain a single "*" wildcard.
	 *
	 * @param value the map to search.
	 * @return the list of StructureMaps.
	 */
	private List<StructureMap> findMatchingMaps(String value) {
		List<StructureMap> result = new ArrayList<>();
		if (value.contains("*")) {
			for (StructureMap structureMap : worker.listTransforms()) {
				if (urlMatches(value, structureMap.getUrl())) {
					result.add(structureMap);
				}
			}
		} else {
			StructureMap structureMap = worker.getTransform(value);
			if (structureMap != null) {
				result.add(structureMap);
			}
		}
		if (result.stream().map(StructureMap::getUrl).distinct().count() != result.size()) {
			throw new InvalidRequestException("Duplicate imports in StructureMap !");
		}

		return result;
	}

	/**
	 * Match a URL with a mask including a single "*" wildcard.
	 *
	 * @param mask the mask to match.
	 * @param url  the URL.
	 * @return true if matches.
	 */
	private boolean urlMatches(String mask, String url) {
		return url.length() > mask.length() &&
			url.startsWith(mask.substring(0, mask.indexOf("*"))) &&
			url.endsWith(mask.substring(mask.indexOf("*") + 1));
	}

	/**
	 * Get String value of a param. Throw an Exception if null.
	 *
	 * @param variables the variables.
	 * @param parameter the parameter.
	 * @param message   the context message.
	 * @return the String value.
	 */
	private String getParamStringNoNull(Variables variables,
													StructureMap.StructureMapGroupRuleTargetParameterComponent parameter,
													String message) {
		Base parameterValue = getParameterValue(variables, parameter);
		if (parameterValue == null) {
			throw new FHIRException(String.format("Unable to find a value for %s. Context: %s", parameter, message));
		}
		if (!parameterValue.hasPrimitiveValue()) {
			throw new FHIRException(String.format("Found a value for %s, but it has a type of %s and cannot be treated as a string. Context: %s",
				parameter, parameterValue.fhirType(), message));
		}
		return parameterValue.primitiveValue();
	}

	/**
	 * Get a parameter value.
	 *
	 * @param vars      the variables.
	 * @param parameter the parameter.
	 * @return the value as a Base FHIR element.
	 */
	private Base getParameterValue(Variables vars, StructureMap.StructureMapGroupRuleTargetParameterComponent parameter) {
		Type type = parameter.getValue();
		if (!(type instanceof IdType)) {
			return type;
		} else {
			String variableName = ((IdType) type).asStringValue();
			Base variable = (Base) vars.get(INPUT, variableName);
			if (variable == null) {
				variable = (Base) vars.get(OUTPUT, variableName);
			}
			if (variable == null) {
				throw new DefinitionException(String.format("Variable %s not found", variableName));
			}
			return variable;
		}
	}

	/**
	 * Evaluate a FHIRPath expression for the target of a Rule.
	 *
	 * @param context the Mapping Context.
	 * @return the evaluated element.
	 */
	private Base evaluateTransform(MappingContext context) {
		ExpressionNode expression = (ExpressionNode) context.getTarget().getUserData(MAP_EXPRESSION);
		if (expression == null) {
			expression = fhirPathEngine.parse(getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(1), context.getTarget().toString()));
			context.getTarget().setUserData(MAP_EXPRESSION, expression);
		}
		List<Base> values = fhirPathEngine.evaluate(context.getVariables(), null, null,
			context.getTarget().getParameter().size() == 2 ?
				getParameterValue(context.getVariables(), context.getTarget().getParameter().get(0)) :
				new BooleanType(false),
			expression);
		if (values.isEmpty()) {
			return null;
		} else if (values.size() != 1) {
			throw new FHIRException(String.format("Rule \"%s\": Evaluation of %s returned %s objects",
				context.getRule().getName(), expression.toString(), values.size()));
		} else {
			return values.get(0);
		}
	}

	/**
	 * Build a Coding element.
	 *
	 * @param uri  the uri.
	 * @param code the code.
	 * @return the Coding element.
	 */
	private Coding buildCoding(String uri, String code, String display) {
//        String system = null;
//        String display = null;
//        ValueSet valueSet = Utilities.noString(uri) ? null : worker.fetchResourceWithException(ValueSet.class, uri);
//        if (valueSet != null) {
//            ValueSetExpander.ValueSetExpansionOutcome expended = worker.expandVS(valueSet, true, false);
//            if (expended.getError() != null) {
//                throw new FHIRException(expended.getError());
//            }
//            CommaSeparatedStringBuilder codes = new CommaSeparatedStringBuilder();
//            for (ValueSet.ValueSetExpansionContainsComponent expend : expended.getValueset().getExpansion().getContains()) {
//                if (expend.hasCode()) {
//                    codes.append(expend.getCode());
//                }
//                if (code.equals(expend.getCode()) && expend.hasSystem()) {
//                    system = expend.getSystem();
//                    display = expend.getDisplay();
//                    break;
//                }
//                if (code.equalsIgnoreCase(expend.getDisplay()) && expend.hasSystem()) {
//                    system = expend.getSystem();
//                    display = expend.getDisplay();
//                    break;
//                }
//            }
//            if (system == null) {
//                throw new FHIRException(String.format("The code '%s' is not in the value set '%s' (valid codes: %s; also checked displays)",
//                        code, uri, codes));
//            }
//        } else {
//            system = uri;
//        }
//        IWorkerContext.ValidationResult validationResult = worker.validateCode(terminologyServiceOptions, system, code, null);
//        if (validationResult != null && validationResult.getDisplay() != null) {
//            display = validationResult.getDisplay();
//        }
//        return new Coding().setSystem(uri).setCode(code).setDisplay(display);
		return new Coding().setSystem(uri).setCode(code).setDisplay(display);
	}

	/**
	 * Translate a concept.
	 *
	 * @param context the Mapping Concept.
	 * @return the translated concept.
	 */
	private Base translate(MappingContext context) {
		Base source = getParameterValue(context.getVariables(), context.getTarget().getParameter().get(0));
		String conceptMapUrl = getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(1), context.getTarget().getParameter().toString());
		String field = context.getTarget().getParameter().size() > 2
			? getParamStringNoNull(context.getVariables(), context.getTarget().getParameter().get(2), context.getTarget().getParameter().toString())
			: null;
		return translate(context, source, conceptMapUrl, field);
	}

	/**
	 * Translate a concept.
	 *
	 * @param context       the Mapping Context.
	 * @param source        the source element.
	 * @param conceptMapUrl the ConceptMap URL for the translation.
	 * @param fieldToReturn the field to return.
	 * @return the translated element.
	 */
	private Base translate(MappingContext context, Base source, String conceptMapUrl, String fieldToReturn) {
		Coding src = new Coding();
		if (source.isPrimitive()) {
			src.setCode(source.primitiveValue());
		} else if ("Coding".equals(source.fhirType())) {
			Base[] b = source.getProperty("system".hashCode(), "system", true);
			if (b.length == 1) {
				src.setSystem(b[0].primitiveValue());
			}
			b = source.getProperty("code".hashCode(), "code", true);
			if (b.length == 1) {
				src.setCode(b[0].primitiveValue());
			}
		} else if ("CE".equals(source.fhirType())) {
			Base[] b = source.getProperty("codeSystem".hashCode(), "codeSystem", true);
			if (b.length == 1) {
				src.setSystem(b[0].primitiveValue());
			}
			b = source.getProperty("code".hashCode(), "code", true);
			if (b.length == 1) {
				src.setCode(b[0].primitiveValue());
			}
		} else {
			throw new FHIRException("Unable to translate source " + source.fhirType());
		}

		String su = conceptMapUrl;
		if (conceptMapUrl.equals("http://hl7.org/fhir/ConceptMap/special-oid2uri")) {
			String uri = worker.oid2Uri(src.getCode());
			if (uri == null)
				uri = "urn:oid:" + src.getCode();
			if ("uri".equals(fieldToReturn))
				return new UriType(uri);
			else
				throw new FHIRException("Error in return code");
		} else {
			ConceptMap cmap = null;
			if (conceptMapUrl.startsWith("#")) {
				for (Resource r : context.getStructureMap().getContained()) {
					if (r instanceof ConceptMap && r.getId().equals(conceptMapUrl)) {
						cmap = (ConceptMap) r;
						su = context.getStructureMap().getUrl() + "#" + conceptMapUrl;
					}
				}
				if (cmap == null)
					throw new FHIRException("Unable to translate - cannot find map " + conceptMapUrl);
			} else {
				if (conceptMapUrl.contains("#")) {
					String[] p = conceptMapUrl.split("\\#");
					StructureMap mapU = worker.fetchResource(StructureMap.class, p[0]);
					if (mapU != null) {
						for (Resource r : mapU.getContained()) {
							if (r instanceof ConceptMap && r.getId().equals(p[1])) {
								cmap = (ConceptMap) r;
								su = conceptMapUrl;
							}
						}
					}
				}
				if (cmap == null) {
					cmap = worker.fetchResource(ConceptMap.class, conceptMapUrl);
				}
			}
			Coding outcome = null;
			boolean done = false;
			String message = null;
			if (cmap == null) {
				if (services == null) {
					message = "No map found for " + conceptMapUrl;
				} else {
					outcome = services.translate(context.getAppInfo(), src, conceptMapUrl);
					done = true;
				}
			} else {
				List<SourceElementComponentWrapper> list = new ArrayList<SourceElementComponentWrapper>();
				for (ConceptMap.ConceptMapGroupComponent g : cmap.getGroup()) {
					for (ConceptMap.SourceElementComponent e : g.getElement()) {
						if (!src.hasSystem() && src.getCode().equals(e.getCode())) {
							list.add(new SourceElementComponentWrapper(g, e));
						} else if (src.hasSystem() && src.getSystem().equals(g.getSource()) && src.getCode().equals(e.getCode())) {
							list.add(new SourceElementComponentWrapper(g, e));
						}
					}
				}
				if (list.isEmpty()) {
					done = true;
				} else if (list.get(0).comp.getTarget().isEmpty()) {
					message = "Concept map " + su + " found no translation for " + src.getCode();
				} else {
					for (ConceptMap.TargetElementComponent tgt : list.get(0).comp.getTarget()) {
						if (tgt.getEquivalence() == null || EnumSet.of(Enumerations.ConceptMapEquivalence.EQUAL, Enumerations.ConceptMapEquivalence.RELATEDTO, Enumerations.ConceptMapEquivalence.EQUIVALENT, Enumerations.ConceptMapEquivalence.WIDER).contains(tgt.getEquivalence())) {
							if (done) {
								message = "Concept map " + su + " found multiple matches for " + src.getCode();
								done = false;
							} else {
								done = true;
								outcome = new Coding().setCode(tgt.getCode()).setSystem(list.get(0).group.getTarget());
							}
						} else if (tgt.getEquivalence() == Enumerations.ConceptMapEquivalence.UNMATCHED) {
							done = true;
						}
					}
					if (!done) {
						message = "Concept map " + su + " found no usable translation for " + src.getCode();
					}
				}
			}
			if (!done) {
				throw new FHIRException(message);
			}
			if (outcome == null) {
				return null;
			}
			if ("code".equals(fieldToReturn)) {
				return new CodeType(outcome.getCode());
			} else {
				return outcome;
			}
		}
	}

	/**
	 * Resolve a group by type.
	 *
	 * @param context    the Mapping Context.
	 * @param sourceType the source type.
	 * @param targetType the target type.
	 * @return the resolved group.
	 */
	private ResolvedGroup resolveGroupByTypes(MappingContext context, String sourceType, String targetType) {
		String kn = "types^" + sourceType + ":" + targetType;

		if (context.getGroup().hasUserData(kn)) {
			return (ResolvedGroup) context.getGroup().getUserData(kn);
		}

		ResolvedGroup resolvedGroup = new ResolvedGroup();
		for (StructureMap.StructureMapGroupComponent grp : context.getStructureMap().getGroup()) {
			if (matchesByType(context.getStructureMap(), grp, sourceType, targetType)) {
				if (resolvedGroup.targetMap == null) {
					resolvedGroup.targetMap = context.getStructureMap();
					resolvedGroup.target = grp;
				} else
					throw new FHIRException(String.format("Multiple possible matches looking for rule for '%s/%s', from rule '%s'",
						sourceType, targetType, context.getRule().getName()));
			}
		}
		if (resolvedGroup.targetMap != null) {
			context.getGroup().setUserData(kn, resolvedGroup);
			return resolvedGroup;
		}

		for (UriType importMap : context.getStructureMap().getImport()) {
			List<StructureMap> mapList = findMatchingMaps(importMap.getValue());
			if (mapList.isEmpty()) {
				throw new FHIRException("Unable to find map(s) for " + importMap.getValue());
			}
			for (StructureMap impMap : mapList) {
				if (!impMap.getUrl().equals(context.getStructureMap().getUrl())) {
					for (StructureMap.StructureMapGroupComponent grp : impMap.getGroup()) {
						if (matchesByType(impMap, grp, sourceType, targetType)) {
							if (resolvedGroup.targetMap == null) {
								resolvedGroup.targetMap = impMap;
								resolvedGroup.target = grp;
							} else
								throw new FHIRException(String.format("Multiple possible matches for rule for '%s/%s' in %s and %s, from rule '%s'",
									sourceType, targetType, resolvedGroup.targetMap.getUrl(), impMap.getUrl(), context.getRule().getName()));
						}
					}
				}
			}
		}
		if (resolvedGroup.target == null) {
			throw new FHIRException(String.format("No matches found for rule for '%s to %s' from %s, from rule '%s'",
				sourceType, targetType, context.getStructureMap().getUrl(), context.getRule().getName()));
		}
		context.getGroup().setUserData(kn, resolvedGroup);
		return resolvedGroup;
	}

	/**
	 * Resolve a reference to a group.
	 *
	 * @param context the Mapping context.
	 * @param name    the name for the reference.
	 * @return the resolved group.
	 */
	private ResolvedGroup resolveGroupReference(MappingContext context, String name) {
		String kn = "ref^" + name;
		if (context.getGroup().hasUserData(kn)) {
			return (ResolvedGroup) context.getGroup().getUserData(kn);
		}

		ResolvedGroup resolvedGroup = new ResolvedGroup();
		resolvedGroup.targetMap = null;
		resolvedGroup.target = null;
		for (StructureMap.StructureMapGroupComponent group : context.getStructureMap().getGroup()) {
			if (group.getName().equals(name)) {
				if (resolvedGroup.targetMap == null) {
					resolvedGroup.targetMap = context.getStructureMap();
					resolvedGroup.target = group;
				} else {
					throw new FHIRException(String.format("Multiple possible matches for group '%s'",
						name));
				}
			}
		}
		if (resolvedGroup.targetMap != null) {
			context.getGroup().setUserData(kn, resolvedGroup);
			return resolvedGroup;
		}

		for (UriType imp : context.getStructureMap().getImport()) {
			List<StructureMap> impMapList = findMatchingMaps(imp.getValue());
			if (impMapList.isEmpty()) {
				throw new FHIRException(String.format("Unable to find map(s) for %s", imp.getValue()));
			}
			for (StructureMap impMap : impMapList) {
				if (!impMap.getUrl().equals(context.getStructureMap().getUrl())) {
					for (StructureMap.StructureMapGroupComponent group : impMap.getGroup()) {
						if (group.getName().equals(name)) {
							if (resolvedGroup.targetMap == null) {
								resolvedGroup.targetMap = impMap;
								resolvedGroup.target = group;
							} else {
								throw new FHIRException(String.format("Multiple possible matches for rule group '%s' in %s#%s and %s#%s",
									name, resolvedGroup.targetMap.getUrl(), resolvedGroup.target.getName(), impMap.getUrl(), group.getName()));
							}
						}
					}
				}
			}
		}
		if (resolvedGroup.target == null) {
			throw new FHIRException(String.format("No matches found for rule '%s'. Reference found in %s",
				name, context.getStructureMap().getUrl()));
		}
		context.getGroup().setUserData(kn, resolvedGroup);
		return resolvedGroup;
	}

	/**
	 * Execute dependency on groups.
	 *
	 * @param context   the Mapping Context.
	 * @param dependent the dependent.
	 */
	private void executeDependency(MappingContext context, StructureMap.StructureMapGroupRuleDependentComponent dependent) {
		ResolvedGroup resolvedGroup = resolveGroupReference(context, dependent.getName());

		if (resolvedGroup.target.getInput().size() != dependent.getVariable().size()) {
			throw new FHIRException(String.format("Rule '%s' has %s but the invocation has %s variables",
				dependent.getName(), resolvedGroup.target.getInput().size(), dependent.getVariable().size()));
		}
		Variables variables = new Variables();
		for (int i = 0; i < resolvedGroup.target.getInput().size(); i++) {
			StructureMap.StructureMapGroupInputComponent input = resolvedGroup.target.getInput().get(i);
			StringType rdp = dependent.getVariable().get(i);
			String var = rdp.asStringValue();
			Variable.VariableMode mode = input.getMode() == StructureMap.StructureMapInputMode.SOURCE ? INPUT : OUTPUT;
			Base vv = (Base) context.getVariables().get(mode, var);
			if (vv == null && mode == INPUT) {
				vv = (Base) context.getVariables().get(OUTPUT, var);
			}
			if (vv == null) {
				throw new FHIRException(String.format("Rule '%s' %s variable '%s' named as '%s' has no value",
					dependent.getName(), mode, input.getName(), var));
			}
			variables.add(mode, input.getName(), vv);
		}
		MappingContext childContext = new MappingContext(resolvedGroup.targetMap, resolvedGroup.target, variables);
		executeGroup(childContext, false);
	}

	/**
	 * Set the property with depth in the path. Allows to instantiate intermediate elements and list Items.
	 *
	 * @param context         the Mapping Context.
	 * @param targetedElement the targeted Element.
	 * @param property        the property to set in the targeted Element.
	 * @return the property as set in the Element.
	 */
	private Base setProperty(MappingContext context, Base targetedElement, Base property) {
		String[] splitElementPath = context.getTarget().getElement().split("\\.");
		Base element = targetedElement;

		for (int i = 0; i < splitElementPath.length; i++) {
			String elementPath = splitElementPath[i];
			//Remove indexes as it is not handeled yet !
			if (elementPath.contains("[") && i < (splitElementPath.length - 1)) {
				String index = elementPath.substring(elementPath.indexOf('[') + 1, elementPath.indexOf(']'));
				elementPath = elementPath.substring(0, elementPath.indexOf('['));

				if ("+".equals(index)) {
					element = element.makeProperty(elementPath.hashCode(), elementPath);
				} else if ("=".equals(index)) {
					Base[] elements = element.getProperty(elementPath.hashCode(), elementPath, true);
					if (elements.length == 0) {
						throw new InvalidRequestException(String.format("Element index does not exist ! \"%s\" in \"%s\"", elementPath, context.getTarget().getElement()));
					}
					element = elements[elements.length - 1];
				} else {
					try {
						int parsedIndex = Integer.parseInt(index);
						Base[] elements = element.getProperty(elementPath.hashCode(), elementPath, true);
						if (elements.length <= parsedIndex && parsedIndex != 0) {
							throw new InvalidRequestException(String.format("Element index does not exist ! \"%s\" in \"%s\"", elementPath, context.getTarget().getElement()));
						} else if (elements.length > parsedIndex) {
							element = elements[parsedIndex];
						} else {
							element = element.makeProperty(elementPath.hashCode(), elementPath);
						}
					} catch (NumberFormatException e) {
						throw new InvalidRequestException(String.format("Element index shall be an integer or \"+\" or \"=\" ! \"%s\" in \"%s\"",
							elementPath, context.getTarget().getElement()));
					}
				}
			} else if (elementPath.contains("[")) {
				throw new InvalidRequestException(String.format("Element cannot end with index ! in \"%s\"", context.getTarget().getElement()));
			} else {
				if (i < (splitElementPath.length - 1)) {
					Base[] propertyArray = element.getProperty(elementPath.hashCode(), elementPath, true);
					if (propertyArray.length > 0) {
						element = propertyArray[0];
					} else {
						element = element.makeProperty(elementPath.hashCode(), elementPath);
					}
				} else {
					element = element.setProperty(elementPath.hashCode(), elementPath, property);
				}
			}
		}
		return element;
	}

	private static class SourceElementComponentWrapper {

		private final ConceptMap.ConceptMapGroupComponent group;
		private final ConceptMap.SourceElementComponent comp;

		public SourceElementComponentWrapper(ConceptMap.ConceptMapGroupComponent group, ConceptMap.SourceElementComponent comp) {
			this.group = group;
			this.comp = comp;
		}
	}
}
