package ca.uhn.fhir.jpa.starter.mapping.validation;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.util.BundleUtil;
import org.apache.commons.lang3.Validate;
import org.hl7.fhir.common.hapi.validation.support.RemoteTerminologyServiceValidationSupport;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeSystem;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class ExtendedRemoteTerminologyServiceValidationSupport extends RemoteTerminologyServiceValidationSupport {

    private String baseUrl;
    private List<Object> myClientInterceptors = new ArrayList<>();

    public ExtendedRemoteTerminologyServiceValidationSupport(FhirContext theFhirContext, String theBaseUrl) {
        super(theFhirContext, theBaseUrl);
        this.baseUrl = theBaseUrl;
    }

    @Override
    public <T extends IBaseResource> T fetchResource(@Nullable Class<T> theClass, String theUri) {
        Validate.notBlank(theUri, "theUri must not be null or blank");

        if (theClass == null) {
            Supplier<IBaseResource>[] sources = new Supplier[]{
                    () -> fetchStructureDefinition(theUri),
                    () -> fetchValueSet(theUri),
                    () -> fetchCodeSystem(theUri)
            };
            return (T) Arrays
                    .stream(sources)
                    .map(t -> t.get())
                    .filter(t -> t != null)
                    .findFirst()
                    .orElse(null);
        }

        switch (getFhirContext().getResourceType(theClass)) {
            case "StructureDefinition":
                return theClass.cast(fetchStructureDefinition(theUri));
            case "ValueSet":
                return theClass.cast(fetchValueSet(theUri));
            case "CodeSystem":
                return theClass.cast(fetchCodeSystem(theUri));
            case "ConceptMap":
                return theClass.cast(fetchConceptMap(theUri));
        }

        if (theUri.startsWith(URL_PREFIX_VALUE_SET)) {
            return theClass.cast(fetchValueSet(theUri));
        }

        return null;
    }

    public IBaseResource fetchConceptMap(String conceptMapUrl) {
        IGenericClient client = provideClient();
        Class<? extends IBaseBundle> bundleType = myCtx.getResourceDefinition("Bundle").getImplementingClass(IBaseBundle.class);
        IBaseBundle results = client
                .search()
                .forResource("ConceptMap")
                .where(CodeSystem.URL.matches().value(conceptMapUrl))
                .returnBundle(bundleType)
                .execute();
        List<IBaseResource> resultsList = BundleUtil.toListOfResources(myCtx, results);
        if (resultsList.size() > 0) {
            return resultsList.get(0);
        }

        return null;
    }

    private IGenericClient provideClient() {
        IGenericClient retVal = myCtx.newRestfulGenericClient(baseUrl);
        for (Object next : myClientInterceptors) {
            retVal.registerInterceptor(next);
        }
        return retVal;
    }

    public void addClientInterceptor(@Nonnull Object theClientInterceptor) {
        super.addClientInterceptor(theClientInterceptor);
        Validate.notNull(theClientInterceptor, "theClientInterceptor must not be null");
        myClientInterceptors.add(theClientInterceptor);
    }
}
