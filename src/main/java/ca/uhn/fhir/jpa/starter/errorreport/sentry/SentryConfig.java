package ca.uhn.fhir.jpa.starter.errorreport.sentry;

import ca.uhn.fhir.jpa.starter.AppProperties;
import ca.uhn.fhir.jpa.starter.errorreport.ErrorReporter;
import ca.uhn.fhir.jpa.starter.errorreport.ErrorReportingExceptionInterceptor;
import io.sentry.Sentry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional({OnSentryEnabled.class})
public class SentryConfig {

	private static final Logger log = LoggerFactory.getLogger(SentryConfig.class);

	private AppProperties.SentryProperties sentryProperties;

	public SentryConfig(AppProperties appProperties) {
		sentryProperties = appProperties.getSentry();

		log.info("Sentry configuration activated.");
		log.info("Sentry DSN present: {}", sentryProperties.getDsn() != null && !sentryProperties.getDsn().isEmpty());
		log.info("Environment: {}", sentryProperties.getEnvironment());
		log.info("Release: {}", sentryProperties.getRelease());
		log.info("Service name: {}", sentryProperties.getServiceName());
		log.info("Tracing enabled: {}", sentryProperties.isTracesEnabled());
		log.info("Tracing sample rate: {}", sentryProperties.getTracesSampleRate());
	}

	@PostConstruct
	public void initSentry() {
		// Defensive: your condition should guarantee this, but keep it safe.
		if (sentryProperties == null || !sentryProperties.isEnabled() || sentryProperties.getDsn() == null || sentryProperties.getDsn().isBlank()) {
			log.info("Sentry not initialized (disabled or missing DSN).");
			return;
		}
		try {
			Sentry.init(options -> {
				options.setDsn(sentryProperties.getDsn());
				options.setEnvironment(sentryProperties.getEnvironment());  // e.g. dev/staging/prod
				options.setRelease(sentryProperties.getRelease());          // e.g. git sha / version
				options.setServerName(sentryProperties.getServiceName());   // stable per service

				// Privacy defaults
				options.setSendDefaultPii(false);                 // keep off in healthcare contexts
				// (We’ll add beforeSend/beforeBreadcrumb scrubbers next)

				// Tracing (optional)
				if (sentryProperties.isTracesEnabled()) {
					options.setTracesSampleRate(sentryProperties.getTracesSampleRate()); // 0.0 - 1.0
				} else {
					options.setTracesSampleRate(0.0);
				}
			});
			log.info("Sentry initialized for service={}, env={}, release={}",
				sentryProperties.getServiceName(), sentryProperties.getEnvironment(), sentryProperties.getRelease());
		} catch (Exception e) {
			log.error("Error initializing Sentry", e);
		}
	}

	@Bean
	public ErrorReporter errorReporter() {
		return new SentryErrorReporter();
	}

	@Bean
	public ErrorReportingExceptionInterceptor errorReportingExceptionInterceptor(ErrorReporter errorReporter) {
		return new ErrorReportingExceptionInterceptor(errorReporter);
	}
}
