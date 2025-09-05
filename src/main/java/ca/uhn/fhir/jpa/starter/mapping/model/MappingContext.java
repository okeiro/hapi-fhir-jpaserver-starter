package ca.uhn.fhir.jpa.starter.mapping.model;

import org.hl7.fhir.r4.model.StructureMap;

import java.util.List;

public class MappingContext {

    private Object appInfo;
    private StructureMap structureMap;
    private StructureMap.StructureMapGroupComponent group;
    private Variables variables;

    private StructureMap.StructureMapGroupRuleComponent rule;
    private List<StructureMap.StructureMapGroupRuleSourceComponent> sources;
    private StructureMap.StructureMapGroupRuleTargetComponent target;

    private MappingContext() {
        //Empty on purpose
    }

    public MappingContext(MappingContext sourceContext) {
        this.appInfo = sourceContext.getAppInfo();
        this.structureMap = sourceContext.getStructureMap();
        this.group = sourceContext.getGroup();
        this.variables = sourceContext.getVariables();
        this.rule = sourceContext.getRule();
        this.sources = sourceContext.getSources();
        this.target = sourceContext.getTarget();
    }

    public MappingContext(StructureMap structureMap,
                          StructureMap.StructureMapGroupComponent group,
                          Variables variables) {
        this.appInfo = null;
        this.structureMap = structureMap;
        this.group = group;
        this.variables = variables;
    }

    public Object getAppInfo() {
        return appInfo;
    }

    public StructureMap getStructureMap() {
        return structureMap;
    }

    public StructureMap.StructureMapGroupComponent getGroup() {
        return group;
    }

    public Variables getVariables() {
        return variables;
    }

    public MappingContext setVariables(Variables variables) {
        this.variables = variables;
        return this;
    }

    public StructureMap.StructureMapGroupRuleComponent getRule() {
        return rule;
    }

    public void setRule(StructureMap.StructureMapGroupRuleComponent rule) {
        this.rule = rule;
    }

    public List<StructureMap.StructureMapGroupRuleSourceComponent> getSources() {
        return sources;
    }

    public void setSources(List<StructureMap.StructureMapGroupRuleSourceComponent> sources) {
        this.sources = sources;
    }

    public StructureMap.StructureMapGroupRuleTargetComponent getTarget() {
        return target;
    }

    public void setTarget(StructureMap.StructureMapGroupRuleTargetComponent target) {
        this.target = target;
    }
}
