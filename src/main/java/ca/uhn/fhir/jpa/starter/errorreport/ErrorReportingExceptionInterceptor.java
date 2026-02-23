package ca.uhn.fhir.jpa.starter.errorreport;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;

@Interceptor
public class ErrorReportingExceptionInterceptor {

	private final ErrorReporter errorReporter;

	public ErrorReportingExceptionInterceptor(ErrorReporter errorReporter) {
		this.errorReporter = errorReporter;
	}

	@Hook(Pointcut.SERVER_HANDLE_EXCEPTION)
	public boolean handleException(RequestDetails requestDetails, BaseServerResponseException ex) {
		errorReporter.reportException(ex);
		return true;
	}
}