package ca.uhn.fhir.jpa.starter.mapping;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class MappingConfigCondition implements Condition {

	@Override
	public boolean matches(ConditionContext theConditionContext, AnnotatedTypeMetadata theAnnotatedTypeMetadata) {
		String property = theConditionContext.getEnvironment().getProperty("hapi.fhir.mapping.enabled");
		return Boolean.parseBoolean(property);
	}
}
