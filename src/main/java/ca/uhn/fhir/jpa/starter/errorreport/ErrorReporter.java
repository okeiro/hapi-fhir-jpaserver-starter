package ca.uhn.fhir.jpa.starter.errorreport;

public interface ErrorReporter {
	void reportException(Throwable t);
	void reportMessage(String message);
}
