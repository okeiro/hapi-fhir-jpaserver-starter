package ca.uhn.fhir.jpa.starter.mapping.model;

public class Variable {

    private final VariableMode mode;
    private final String name;
    private final Object object;

    public Variable(VariableMode mode, String name, Object object) {
        this.mode = mode;
        this.name = name;
        this.object = object;
    }

    public VariableMode getMode() {
        return this.mode;
    }

    public String getName() {
        return this.name;
    }

    public Object getObject() {
        return this.object;
    }

    public enum VariableMode {
        INPUT,
        OUTPUT,
        SHARED
    }
}
