package ca.uhn.fhir.jpa.starter.mapping;

import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.starter.mapping.provider.TransformProvider;
import org.hl7.fhir.r4.model.StructureMap;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional({MappingConfigCondition.class})
public class MappingConfig {

	@Bean
	public TransformProvider ipsOperationProvider(IFhirResourceDao<StructureMap> structureMapDao) {
		return new TransformProvider(structureMapDao);
	}
}
