/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.docs.configuration;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigGroup;
import org.apache.flink.configuration.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.WebOptions;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class used for generating code based documentation of configuration parameters.
 */
public class ConfigOptionsDocGenerator {

	/**
	 * This method generates html tables from set of classes containing {@link ConfigOption ConfigOptions}.
	 *
	 * <p>For each class 1 or more html tables will be generated and placed into a separate file, depending on whether
	 * the class is annotated with {@link ConfigGroups}. The tables contain the key, default value and description for
	 * every {@link ConfigOption}.
	 *
	 * @param args
	 *  [0] output directory for the generated files
	 *  [1] project root directory
	 *  [x] module containing an *Options class
	 *  [x+1] package to the * Options.classes
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String outputDirectory = args[0];
		String rootDir = args[1];
		for (int x = 2; x + 1 < args.length; x += 2) {
			createTable(rootDir, args[x], args[x + 1], outputDirectory);
		}
	}

	private static void createTable(String rootDir, String module, String packageName, String outputDirectory) throws IOException, ClassNotFoundException {
		Path configDir = Paths.get(rootDir, module, "src/main/java", packageName.replaceAll("\\.", "/"));

		Pattern p = Pattern.compile("(([a-zA-Z]*)(Options|Config|Parameters))\\.java");
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
			for (Path entry : stream) {
				String fileName = entry.getFileName().toString();
				Matcher matcher = p.matcher(fileName);
				if (!fileName.equals("ConfigOptions.java") && matcher.matches()) {

					Class<?> optionsClass = Class.forName(packageName + "." + matcher.group(1));

					Path javadocDir = Paths.get(rootDir, module, "target/site/apidocs", packageName.replaceAll("\\.", "/"));
					Map<String, String> javadocs = getFieldJavadocMapping(javadocDir.resolve(javadocDir.resolve(matcher.group(1) + ".html")));

					List<Tuple2<ConfigGroup, String>> tables = generateTablesForClass(optionsClass, javadocs);
					if (tables.size() > 0) {
						for (Tuple2<ConfigGroup, String> group : tables) {

							String name = group.f0 == null
								? matcher.group(2).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase()
								: group.f0.name().replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();

							String outputFile = name + "_configuration.html";
							Files.write(Paths.get(outputDirectory, outputFile), group.f1.getBytes(StandardCharsets.UTF_8));
						}
					}
				}
			}
		}
	}

	@VisibleForTesting
	static List<Tuple2<ConfigGroup, String>> generateTablesForClass(Class<?> optionsClass, Map<String, String> javadocs) {
		ConfigGroups configGroups = optionsClass.getAnnotation(ConfigGroups.class);
		List<Tuple2<ConfigGroup, String>> tables = new ArrayList<>();
		List<DocumentedConfigOption> allOptions = extractConfigOptions(optionsClass, javadocs);

		if (configGroups != null) {
			Tree tree = new Tree(configGroups.groups(), allOptions);

			for (ConfigGroup group : configGroups.groups()) {
				List<DocumentedConfigOption> configOptions = tree.findConfigOptions(group);
				sortOptions(configOptions);
				tables.add(Tuple2.of(group, toHtmlTable(configOptions)));
			}
			List<DocumentedConfigOption> configOptions = tree.getDefaultOptions();
			sortOptions(configOptions);
			tables.add(Tuple2.of(null, toHtmlTable(configOptions)));
		} else {
			sortOptions(allOptions);
			tables.add(Tuple2.of(null, toHtmlTable(allOptions)));
		}
		return tables;
	}

	private static List<DocumentedConfigOption> extractConfigOptions(Class<?> clazz, Map<String, String> javadocs) {
		try {
			List<DocumentedConfigOption> configOptions = new ArrayList<>();
			Field[] fields = clazz.getFields();
			for (Field field : fields) {
				if (field.getType().equals(ConfigOption.class) && field.getAnnotation(Deprecated.class) == null) {
					configOptions.add(new DocumentedConfigOption((ConfigOption) field.get(null), javadocs.get(field.getName())));
				}
			}

			return configOptions;
		} catch (Exception e) {
			throw new RuntimeException("Failed to extract config options from class " + clazz + ".", e);
		}
	}

	private static class DocumentedConfigOption {
		final ConfigOption<?> option;
		final String javadoc;

		private DocumentedConfigOption(ConfigOption<?> option, String javadoc) {
			this.option = option;
			this.javadoc = javadoc;
		}
	}

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param options list of options to include in this group
	 * @return string containing HTML formatted table
	 */
	private static String toHtmlTable(final List<DocumentedConfigOption> options) {
		StringBuilder htmlTable = new StringBuilder();
		htmlTable.append("<table class=\"table table-bordered\">\n");
		htmlTable.append("    <thead>\n");
		htmlTable.append("        <tr>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 65%\">Description</th>\n");
		htmlTable.append("        </tr>\n");
		htmlTable.append("    </thead>\n");
		htmlTable.append("    <tbody>\n");

		for (DocumentedConfigOption option : options) {
			htmlTable.append(toHtmlString(option));
		}

		htmlTable.append("    </tbody>\n");
		htmlTable.append("</table>\n");

		return htmlTable.toString();
	}

	/**
	 * Transforms option to table row.
	 *
	 * @param option option to transform
	 * @return row with the option description
	 */
	private static String toHtmlString(final DocumentedConfigOption option) {
		Object defaultValue = option.option.defaultValue();
		// This is a temporary hack that should be removed once FLINK-6490 is resolved.
		// These options use System.getProperty("java.io.tmpdir") as the default.
		// As a result the generated table contains an actual path as the default, which is simply wrong.
		if (option.option == WebOptions.TMP_DIR || option.option.key().equals("python.dc.tmp.dir")) {
			defaultValue = null;
		}
		return "" +
			"        <tr>\n" +
			"            <td><h5>" + escapeCharacters(option.option.key()) + "</h5></td>\n" +
			"            <td>" + escapeCharacters(defaultValueToHtml(defaultValue)) + "</td>\n" +
			"            <td>" + option.javadoc + "</td>\n" +
			"        </tr>\n";
	}

	private static String defaultValueToHtml(Object value) {
		if (value instanceof String) {
			if (((String) value).isEmpty()) {
				return "(none)";
			}
			return "\"" + value + "\"";
		}

		return value == null ? "(none)" : value.toString();
	}

	private static String escapeCharacters(String value) {
		return value
			.replaceAll("<", "&#60;")
			.replaceAll(">", "&#62;");
	}

	private static void sortOptions(List<DocumentedConfigOption> configOptions) {
		configOptions.sort(Comparator.comparing(o -> o.option.key()));
	}

	private static Map<String, String> getFieldJavadocMapping(Path file) throws IOException {
		Document d = Jsoup.parse(file.toFile(), StandardCharsets.UTF_8.name());

		Element body = d.body();

		Elements memberSummary = body.getElementsByAttribute("summary");

		List<Tuple2<String, String>> collect = memberSummary.stream()
			.flatMap(element -> element.children().stream())
			.filter(element -> element.tagName().equals("tbody"))
			.flatMap(element -> element.children().stream())
			.filter(element -> element.tagName().equals("tr"))
			.filter(element -> element.classNames().contains("altColor") || element.classNames().contains("rowColor"))
			.flatMap(element -> element.children().stream())
			.filter(element -> element.tagName().equals("td") && element.classNames().contains("colLast"))
			.map(ConfigOptionsDocGenerator::extractInfo)
			.collect(Collectors.toList());

		Map<String, String> mapping = new HashMap<>();
		for (Tuple2<String, String> element : collect) {
			mapping.put(element.f0, element.f1 != null ? element.f1 : "");
		}
		return mapping;
	}

	private static Tuple2<String, String> extractInfo(Element field) {

		synchronized (GeneratorV2.class) {
			String fieldName = field.getElementsByTag("code")
				.get(0)
				.getElementsByTag("span")
				.get(0)
				.getElementsByTag("a")
				.get(0)
				.text();

			Elements div = field.getElementsByTag("div");
			String fieldDoc = null;
			if (div.size() > 0) {
				fieldDoc = div.get(0).html();
			}

			return Tuple2.of(fieldName, fieldDoc);
		}
	}
	
	/**
	 * Data structure used to assign {@link ConfigOption ConfigOptions} to the {@link ConfigGroup} with the longest
	 * matching prefix.
	 */
	private static class Tree {
		private final Node root = new Node();

		Tree(ConfigGroup[] groups, Collection<DocumentedConfigOption> options) {
			// generate a tree based on all key prefixes
			for (ConfigGroup group : groups) {
				String[] keyComponents = group.keyPrefix().split("\\.");
				Node currentNode = root;
				for (String keyComponent : keyComponents) {
					currentNode = currentNode.addChild(keyComponent);
				}
				currentNode.markAsGroupRoot();
			}

			// assign options to their corresponding group, i.e. the last group root node encountered when traversing
			// the tree based on the option key
			for (DocumentedConfigOption option : options) {
				findGroupRoot(option.option.key()).assignOption(option);
			}
		}

		List<DocumentedConfigOption> findConfigOptions(ConfigGroup configGroup) {
			Node groupRoot = findGroupRoot(configGroup.keyPrefix());
			return groupRoot.getConfigOptions();
		}

		List<DocumentedConfigOption> getDefaultOptions() {
			return root.getConfigOptions();
		}

		private Node findGroupRoot(String key) {
			String[] keyComponents = key.split("\\.");
			Node currentNode = root;
			for (String keyComponent : keyComponents) {
				currentNode = currentNode.findChild(keyComponent);
			}
			return currentNode.isGroupRoot() ? currentNode : root;
		}

		private static class Node {
			private final List<DocumentedConfigOption> configOptions = new ArrayList<>();
			private final Map<String, Node> children = new HashMap<>();
			private boolean isGroupRoot = false;

			private Node addChild(String keyComponent) {
				Node child = children.get(keyComponent);
				if (child == null) {
					child = new Node();
					children.put(keyComponent, child);
				}
				return child;
			}

			private Node findChild(String keyComponent) {
				Node child = children.get(keyComponent);
				if (child == null) {
					return this;
				}
				return child;
			}

			private void assignOption(DocumentedConfigOption option) {
				configOptions.add(option);
			}

			private boolean isGroupRoot() {
				return isGroupRoot;
			}

			private void markAsGroupRoot() {
				this.isGroupRoot = true;
			}

			private List<DocumentedConfigOption> getConfigOptions() {
				return configOptions;
			}
		}
	}

	private ConfigOptionsDocGenerator() {
	}
}
