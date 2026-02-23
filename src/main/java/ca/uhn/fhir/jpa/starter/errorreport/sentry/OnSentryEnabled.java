package ca.uhn.fhir.jpa.starter.errorreport.sentry;

import ca.uhn.fhir.jpa.starter.AppProperties;
import ca.uhn.fhir.jpa.starter.util.EnvironmentHelper;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class OnSentryEnabled implements Condition {
	@Override
	public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
		var appProperties = EnvironmentHelper.getConfiguration(context, "hapi.fhir", AppProperties.class);
		if (appProperties == null) return false;
		AppProperties.SentryProperties sentryProperties = appProperties.getSentry();
		return sentryProperties != null
			&& sentryProperties.isEnabled()
			&& sentryProperties.getDsn() != null
			&& !sentryProperties.getDsn().isEmpty();
	}
}
