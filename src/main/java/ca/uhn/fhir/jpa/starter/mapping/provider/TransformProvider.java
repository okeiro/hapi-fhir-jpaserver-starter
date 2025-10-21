package ca.uhn.fhir.jpa.starter.mapping.provider;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.DefaultProfileValidationSupport;
import ca.uhn.fhir.context.support.IValidationSupport;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.jpa.starter.mapping.service.FFHIRPathHostServices;
import ca.uhn.fhir.jpa.starter.mapping.service.Mapper;
import ca.uhn.fhir.jpa.starter.mapping.validation.ExtendedRemoteTerminologyServiceValidationSupport;
import ca.uhn.fhir.jpa.starter.mapping.validation.PersistedValidationSupportClass;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.param.UriParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.common.hapi.validation.support.ValidationSupportChain;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;
import org.hl7.fhir.r4.hapi.ctx.HapiWorkerContext;
import org.hl7.fhir.r4.model.Endpoint;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StructureMap;
import org.hl7.fhir.r4.model.UriType;

import java.util.List;


public class TransformProvider {

	private final IFhirResourceDao<StructureMap> myStructureMapDao;

	private PersistedValidationSupportClass validationSupport;

	public TransformProvider(IFhirResourceDao<StructureMap> theStructureMapDao) {
		myStructureMapDao = theStructureMapDao;
	}

	@Operation(name = "$transform")
	public Parameters transform(@ResourceParam Parameters parameters) {

		FhirContext context = FhirContext.forR4();
		ValidationSupportChain validationSupport = new ValidationSupportChain(
			getValidationSupport(),
			new DefaultProfileValidationSupport(context)
		);

		if (parameters.getParameter("terminologyEndpoint") != null) {
			String terminologyUrl = ((Endpoint) parameters.getParameter("terminologyEndpoint").getResource()).getAddress();
			validationSupport.addValidationSupport(new ExtendedRemoteTerminologyServiceValidationSupport(context, terminologyUrl));
		}

		HapiWorkerContext hapiContext = new HapiWorkerContext(context, validationSupport);

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);
		fhirPathEngine.setHostServices(new FFHIRPathHostServices());

		IGenericClient clientStructureMap = null;
		if (parameters.getParameter("structureMapEndpoint") != null) {
			String structureMapServer = ((Endpoint) parameters.getParameter("structureMapEndpoint").getResource()).getAddress();
			clientStructureMap = context.newRestfulGenericClient(structureMapServer);
		}

		////////////////////////////////////////////////////////////////

		StructureMap structureMap = null;

		if (parameters.getParameter("structureMap") != null) {
			structureMap = ((StructureMap) parameters.getParameter("structureMap").getResource());
		} else if (parameters.getParameter("source") != null) {
			String structureMapUrl = ((UriType) parameters.getParameter("source").getValue()).getValue();
			if (clientStructureMap != null) {
				try {
					structureMap = clientStructureMap
						.search()
						.forResource(StructureMap.class)
						.where(StructureMap.URL.matches().value(structureMapUrl))
						.returnBundle(org.hl7.fhir.r4.model.Bundle.class)
						.execute()
						.getEntry()
						.stream()
						.filter(e -> e.getResource() instanceof StructureMap)
						.map(e -> (StructureMap) e.getResource())
						.findFirst()
						.orElse(null);
				} catch (Exception e) {
					throw new InvalidRequestException("Failed to fetch StructureMap from remote server: " + structureMapUrl, e);
				}
			} else {
				IBundleProvider search = myStructureMapDao.search(new SearchParameterMap()
					.add("url", new UriParam(structureMapUrl)));

				if (search.isEmpty()) {
					throw new InvalidRequestException(String.format("Did not find StructureMap with url '%s' !", structureMapUrl));
				} else if (search.size() > 1) {
					throw new InvalidRequestException(String.format("Found multiple StructureMap with url '%s' !", structureMapUrl));
				}

				List<IBaseResource> resources = search.getResources(0, 1);

				structureMap = (StructureMap) resources.get(0);
			}
		} else {
			throw new InvalidRequestException("No StructureMap parameter");
		}

		Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null, myStructureMapDao, clientStructureMap);

		////////////////////////////////////////////////////////////////

		return mapper.map(structureMap, parameters);
	}

	public IValidationSupport getValidationSupport() {
		return validationSupport;
	}
}
