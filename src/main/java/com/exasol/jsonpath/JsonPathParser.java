package com.exasol.jsonpath;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonPathParser {

	public static List<JsonPathElement> parseJsonPath(String jsonPathSpecification) {
		List<JsonPathElement> path = new ArrayList<>();
		
		// Three capture groups:
		// 1) quoted field names "field.name"
		// 2) list indices in square brackets [i]
		// 3) unquoted field names (may not contain . or [)
		String regex = "\"([^\"]*)\"|\\[(\\d+)\\]|\\[(\\*)\\]|([^\\.\\[]+)";
		Matcher matcher = Pattern.compile(regex).matcher(jsonPathSpecification);
		while (matcher.find()) {
			if (matcher.group(1) != null) {
				if (matcher.group(1).equals("$")) {
					// root element. Root element is always implicitly added, we don't add an element for it
				} else {
					path.add(new JsonPathFieldElement(matcher.group(1)));
				}
			} else if (matcher.group(2) != null) {
				path.add(new JsonPathListIndexElement(Integer.parseInt(matcher.group(2))));
			} else if (matcher.group(3) != null) {
				path.add(new JsonPathListWildcardOperatorElement());
			} else {
				assert(matcher.group(4) != null);
				if (matcher.group(4).equals("$")) {
					// root element. Root element is always implicitly added, we don't add an element for it
				} else {
					path.add(new JsonPathFieldElement(matcher.group(4)));
				}
			}
		}
		return path;
	}
}
