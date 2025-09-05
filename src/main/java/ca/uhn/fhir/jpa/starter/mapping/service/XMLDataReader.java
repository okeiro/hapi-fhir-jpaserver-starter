package ca.uhn.fhir.jpa.starter.mapping.service;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class XMLDataReader {

    private static final Logger logger = LoggerFactory.getLogger(XMLDataReader.class);

    private static Map<String, Object> xmlToMap(Element element) {
        Map<String, Object> map = new HashMap<>();
        NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element childElement = (Element) node;
                String tagName = childElement.getTagName();
                Object value = xmlToMap(childElement);
                if (childElement.getChildNodes().getLength() == 1 && childElement.getFirstChild().getNodeType() == Node.TEXT_NODE) {
                    value = childElement.getTextContent();
                }
                if (map.containsKey(tagName)) {
                    Object existingValue = map.get(tagName);
                    if (existingValue instanceof List) {
                        ((List<Object>) existingValue).add(value);
                    } else {
                        List<Object> list = new ArrayList<>();
                        list.add(existingValue);
                        list.add(value);
                        map.put(tagName, list);
                    }
                } else {
                    map.put(tagName, value);
                }
            }
        }
        return map;
    }

        public static JSONObject parseXMLData(String content) {
        try {
            String xmlContent = new String(Base64.getDecoder().decode(content), StandardCharsets.UTF_8);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder;
            builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(xmlContent)));
            return new JSONObject(xmlToMap(document.getDocumentElement()));
        } catch (Exception e) {
            logger.error("Error while reading XML Data !", e);
            throw new InternalErrorException("Error while reading XML Data !");
        }
    }
}
