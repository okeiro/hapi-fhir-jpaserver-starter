package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.utils.StructureMapUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TransformerService implements StructureMapUtilities.ITransformerServices {

	private static final Logger logger = LoggerFactory.getLogger(TransformerService.class);

	private FhirContext fhirContext;
	private IGenericClient terminologyClient;

	public TransformerService(String terminologyUrl) {
		this.fhirContext = FhirContext.forR4Cached();
		this.terminologyClient = fhirContext.newRestfulGenericClient(terminologyUrl);
	}

	@Override
	public void log(String s) {
		logger.info(s);
	}

	@Override
	public Base createType(Object o, String s) throws FHIRException {
		return ResourceFactory.createResourceOrType(s);
	}

	@Override
	public Base createResource(Object o, Base base, boolean b) {
		//No actual creation here
		return base;
	}

	/**
	 * Calls ConceptMap/$translate using a canonical ConceptMap URL
	 *
	 * @param appInfo         Always null in your use case
	 * @param source          Coding to translate
	 * @param conceptMapUrl   Canonical URL of the ConceptMap
	 * @return The translated Coding (first match)
	 * @throws FHIRException if translation fails or no match is found
	 */
	@Override
	public Coding translate(Object appInfo, Coding source, String conceptMapUrl) throws FHIRException {
		if (source == null) {
			throw new FHIRException("Source coding must not be null");
		}

		if (conceptMapUrl == null || conceptMapUrl.isEmpty()) {
			throw new FHIRException("ConceptMap URL must not be null or empty");
		}

		// Build Parameters resource for $translate
		Parameters inParams = new Parameters();
		// source coding
		inParams.addParameter().setName("coding").setValue(source);
		// canonical ConceptMap URL
		inParams.addParameter().setName("url").setValue(new UriType(conceptMapUrl));

		Parameters outParams;

		try {
			outParams = terminologyClient
				.operation()
				.onType(ConceptMap.class)
				.named("$translate")
				.withParameters(inParams)
				.execute();
		} catch (FhirClientConnectionException e) {
			throw new FHIRException("Error calling terminology server", e);
		}

		// Parse response
		// According to spec, result is in:
		// Parameters.parameter(name="match").part(name="concept").valueCoding
		for (Parameters.ParametersParameterComponent param : outParams.getParameter()) {
			if ("match".equals(param.getName())) {
				for (Parameters.ParametersParameterComponent part : param.getPart()) {
					if ("concept".equals(part.getName()) && part.getValue() instanceof Coding) {
						return (Coding) part.getValue();
					}
				}
			}
		}

		return null;
	}

	@Override
	public Base resolveReference(Object o, String s) throws FHIRException {
		return null;
	}

	@Override
	public List<Base> performSearch(Object o, String s) throws FHIRException {
		//No real research implementation for now
		return List.of();
	}
}
