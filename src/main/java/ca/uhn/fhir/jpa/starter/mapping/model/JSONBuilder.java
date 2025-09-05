package ca.uhn.fhir.jpa.starter.mapping.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JSONBuilder {

    private static final Pattern TOKEN_PATTERN = Pattern.compile("([a-zA-Z0-9_]+)(\\[(\\d+)])?");

    private ObjectNode jsonRoot;

    public JSONBuilder() {
        ObjectMapper mapper = new ObjectMapper();
        jsonRoot = mapper.createObjectNode();
    }

    public void putByPath(String path, Object value) {
        Matcher matcher = TOKEN_PATTERN.matcher(path);
        JsonNode current = jsonRoot;
        String key;
        int index;
        boolean isArray;

        while (matcher.find()) {
            key = matcher.group(1);
            isArray = matcher.group(2) != null;
            index = isArray ? Integer.parseInt(matcher.group(3)) : -1;

            if (isArray) {
                ArrayNode array;
                if (!(current).has(key) || !(current).get(key).isArray()) {
                    array = ((ObjectNode) current).putArray(key);
                } else {
                    array = (ArrayNode) (current).get(key);
                }

                //If the array has to few elements, fill with null
                while (array.size() <= index) {
                    array.addNull();
                }

                if (matcher.hitEnd()) {
                    array.set(index, new TextNode((String) value));
                } else {
                    JsonNode child = array.get(index);
                    if (child == null || !child.isObject()) {
                        ObjectNode newNode = array.objectNode();
                        array.set(index, newNode);
                        current = newNode;
                    } else {
                        current = child;
                    }
                }
            } else {
                if (matcher.hitEnd()) {
                    ((ObjectNode) current).set(key, new TextNode((String) value));
                    return;
                } else {
                    if (!current.has(key) || !current.get(key).isObject()) {
                        //Object doesn't exist at path so we create it
                        ObjectNode newNode = ((ObjectNode) current).objectNode();
                        ((ObjectNode) current).set(key, newNode);
                        current = newNode;
                    } else {
                        //Get the element at path
                        current = current.get(key);
                    }
                }
            }
        }
    }

    public String toString() {
        return jsonRoot.toPrettyString();
    }
}
