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
import ca.uhn.fhir.rest.param.UriParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.common.hapi.validation.support.ValidationSupportChain;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.hapi.ctx.HapiWorkerContext;
import org.hl7.fhir.r4.model.Endpoint;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StructureMap;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.fhirpath.FHIRPathEngine;

import java.util.List;


public class TransformProvider {

	private IFhirResourceDao<StructureMap> myStructureMapDao;

	private PersistedValidationSupportClass validationSupport;

	public TransformProvider(IFhirResourceDao<StructureMap> theStructureMapDao) {
		myStructureMapDao = theStructureMapDao;
	}

	@Operation(name = "$transform")
	public Parameters transform(@ResourceParam Parameters parameters) {

		StructureMap structureMap = null;

		if (parameters.getParameter("structureMap") != null ) {
			structureMap = ((StructureMap) parameters.getParameter("structureMap").getResource());
		}

		else if (parameters.getParameter("source") != null ){
			String structureMapUrl = ((UriType) parameters.getParameter("source").getValue()).getValue();
			IBundleProvider search = myStructureMapDao.search(new SearchParameterMap()
				.add("url", new UriParam(structureMapUrl)));

			if (search.isEmpty()) {
				throw new InvalidRequestException(String.format("Did not find StructureMap with url '%s' !", structureMapUrl));
			} else if (search.size() > 1) {
				throw new InvalidRequestException(String.format("Found multiple StructureMap with url '%s' !", structureMapUrl));
			}

			List<IBaseResource> resources = search.getResources(0, 1);

			structureMap = (StructureMap) resources.get(0);
		} else {
			throw new InvalidRequestException("No StructureMap parameter");
		}

		////////////////////////////////////////////////////////////////
		FhirContext context = FhirContext.forR4();
		ValidationSupportChain validationSupport = new ValidationSupportChain(
			getValidationSupport(),
			new DefaultProfileValidationSupport(context)
		);

		if (parameters.getParameter("terminologyEndpoint") != null ) {
			String terminologyUrl = ((Endpoint) parameters.getParameter("terminologyEndpoint").getResource()).getAddress();
			validationSupport.addValidationSupport(new ExtendedRemoteTerminologyServiceValidationSupport(context, terminologyUrl));
		}

		HapiWorkerContext hapiContext = new HapiWorkerContext(context, validationSupport);

		FHIRPathEngine fhirPathEngine = new FHIRPathEngine(hapiContext);
		fhirPathEngine.setHostServices(new FFHIRPathHostServices());

		Mapper mapper = new Mapper(hapiContext, fhirPathEngine, null);

		////////////////////////////////////////////////////////////////

		return mapper.map(structureMap, parameters);
	}

	public IValidationSupport getValidationSupport() {
		return validationSupport;
	}
}
