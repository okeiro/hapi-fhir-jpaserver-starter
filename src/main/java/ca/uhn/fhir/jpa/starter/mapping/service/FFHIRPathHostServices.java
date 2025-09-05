package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.jpa.starter.mapping.model.Variables;
import org.apache.commons.lang3.NotImplementedException;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.exceptions.PathEngineException;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.fhirpath.FHIRPathUtilityClasses;
import org.hl7.fhir.r4.fhirpath.TypeDetails;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.ValueSet;

import java.util.ArrayList;
import java.util.List;

import static ca.uhn.fhir.jpa.starter.mapping.model.Variable.VariableMode.INPUT;
import static ca.uhn.fhir.jpa.starter.mapping.model.Variable.VariableMode.OUTPUT;

public class FFHIRPathHostServices implements FHIRPathEngine.IEvaluationContext {

	public List<Base> resolveConstant(FHIRPathEngine fpe, Object appContext, String name, boolean beforeContext, boolean explicitConstant) throws PathEngineException {
		Variables vars = (Variables) appContext;
		Object res = vars.get(INPUT, name);
		if (res == null)
			res = vars.get(OUTPUT, name);
		List<Base> result = new ArrayList<Base>();
		if (res != null)
			result.add((Base) res);
		return result;
	}

	@Override
	public TypeDetails resolveConstantType(FHIRPathEngine fpe, Object appContext, String name, boolean explicitConstant) throws PathEngineException {
		throw new Error("Not Implemented Yet");
	}

	@Override
	public boolean log(String argument, List<Base> focus) {
		throw new Error("Not Implemented Yet");
	}

	@Override
	public FHIRPathUtilityClasses.FunctionDetails resolveFunction(FHIRPathEngine fpe, String functionName) {
		return null; // throw new Error("Not Implemented Yet");
	}

	@Override
	public TypeDetails checkFunction(FHIRPathEngine engine, Object appContext, String functionName, TypeDetails focus, List<TypeDetails> parameters) throws PathEngineException {
		throw new Error("Not Implemented Yet");
	}

	@Override
	public List<Base> executeFunction(FHIRPathEngine fpe, Object appContext, List<Base> focus, String functionName, List<List<Base>> parameters) {
		throw new Error("Not Implemented Yet");
	}

	@Override
	public Base resolveReference(FHIRPathEngine fpe, Object appContext, String url, Base base) throws FHIRException {
		throw new NotImplementedException("Not done yet (FFHIRPathHostServices.conformsToProfile), when item is element");
	}

	@Override
	public boolean conformsToProfile(FHIRPathEngine fpe, Object appContext, Base item, String url) throws FHIRException {
		throw new NotImplementedException("Not done yet (FFHIRPathHostServices.conformsToProfile), when item is element");
	}

	@Override
	public ValueSet resolveValueSet(FHIRPathEngine fpe, Object appContext, String url) {
		throw new Error("Not Implemented Yet");
	}

	@Override
	public boolean paramIsType(String name, int index) {
		throw new Error("Not Implemented Yet");
	}

}