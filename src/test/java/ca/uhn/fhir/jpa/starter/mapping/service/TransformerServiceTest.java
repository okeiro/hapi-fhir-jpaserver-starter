package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.gclient.*;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

class TransformerServiceTest {

    private TransformerService transformerService;

    @Mock
    private IGenericClient mockClient;

    @Mock
    private IOperation mockOperation;

    @Mock
    private IOperationUnnamed mockOperationWithInput;

    @Mock
    private IOperationUntyped mockOperationNamed;

	 @Mock
	 private IOperationUntypedWithInputAndPartialOutput<Parameters> mockOperationExecute;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        transformerService = new TransformerService("http://fake-terminology-server");

        // Inject mock client via reflection
        Field clientField = TransformerService.class.getDeclaredField("terminologyClient");
        clientField.setAccessible(true);
        clientField.set(transformerService, mockClient);

        when(mockClient.operation()).thenReturn(mockOperation);
        when(mockOperation.onType(ConceptMap.class)).thenReturn(mockOperationWithInput);
        when(mockOperationWithInput.named("$translate")).thenReturn(mockOperationNamed);
        when(mockOperationNamed.withParameters(any(Parameters.class)))
                .thenReturn(mockOperationExecute);
    }

    @Test
    void translate_shouldReturnTranslatedCoding_whenMatchFound() throws Exception {
        Coding source = new Coding("systemA", "codeA", null);
        Coding translated = new Coding("systemB", "codeB", null);

        Parameters response = new Parameters();
        Parameters.ParametersParameterComponent matchParam =
                response.addParameter().setName("match");

        matchParam.addPart()
                .setName("concept")
                .setValue(translated);

        when(mockOperationExecute.execute()).thenReturn(response);

        Coding result = transformerService.translate(null, source, "http://conceptmap");

        assertNotNull(result);
        assertEquals("systemB", result.getSystem());
        assertEquals("codeB", result.getCode());
    }

    @Test
    void translate_shouldThrowException_whenSourceIsNull() {
        assertThrows(FHIRException.class,
                () -> transformerService.translate(null, null, "http://conceptmap"));
    }

    @Test
    void translate_shouldThrowException_whenConceptMapUrlIsEmpty() {
        Coding source = new Coding("system", "code", null);

        assertThrows(FHIRException.class,
                () -> transformerService.translate(null, source, ""));
    }

    @Test
    void translate_shouldReturnNull_whenNoMatchFound() throws Exception {
        Coding source = new Coding("system", "code", null);

        Parameters emptyResponse = new Parameters();
        when(mockOperationExecute.execute()).thenReturn(emptyResponse);

		 Coding translate = transformerService.translate(null, source, "http://conceptmap");

		 assertNull(translate);
	 }

    @Test
    void translate_shouldThrowException_whenServerFails() throws Exception {
        Coding source = new Coding("system", "code", null);

        when(mockOperationExecute.execute())
                .thenThrow(new FhirClientConnectionException("Connection error"));

        assertThrows(FHIRException.class,
                () -> transformerService.translate(null, source, "http://conceptmap"));
    }

    @Test
    void createType_shouldCreateValidResource() throws Exception {
        Base result = transformerService.createType(null, "Patient");
        assertNotNull(result);
        assertTrue(result instanceof Patient);
    }

    @Test
    void performSearch_shouldReturnEmptyList() throws Exception {
        List<Base> result = transformerService.performSearch(null, "query");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void resolveReference_shouldReturnNull() throws Exception {
        Base result = transformerService.resolveReference(null, "reference");
        assertNull(result);
    }
}
