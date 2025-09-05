package ca.uhn.fhir.jpa.starter.mapping.model;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static ca.uhn.fhir.jpa.starter.mapping.model.Variable.VariableMode.OUTPUT;


public class Variables {

    private List<Variable> list = new ArrayList<>();

    public void add(Variable.VariableMode mode, String name, Object object) {
        Variable variable = null;
        list = list.stream()
                .filter(v -> (v.getMode() != mode) || !v.getName().equals(name))
                .collect(Collectors.toList());
        list.add(new Variable(mode, name, object));
    }

    public Variables copy() {
        Variables result = new Variables();
        result.list.addAll(list);
        return result;
    }

    public Object get(Variable.VariableMode mode, String name) {
        for (Variable v : list)
            if ((v.getMode() == mode) && v.getName().equals(name))
                return v.getObject();
        return null;
    }

    public List<Variable> getOutputs() {
        return this.list.stream().filter(v -> OUTPUT.equals(v.getMode())).collect(Collectors.toList());
    }
}
