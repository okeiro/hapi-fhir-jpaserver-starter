package ca.uhn.fhir.jpa.starter.errorreport.sentry;

import ca.uhn.fhir.jpa.starter.errorreport.ErrorReporter;
import io.sentry.Sentry;

public class SentryErrorReporter implements ErrorReporter {

    @Override
    public void reportException(Throwable t) {
        Sentry.captureException(t);
    }

    @Override
    public void reportMessage(String message) {
        Sentry.captureMessage(message);
    }
}