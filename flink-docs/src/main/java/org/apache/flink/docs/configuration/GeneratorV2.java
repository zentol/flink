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

import org.apache.flink.api.java.tuple.Tuple2;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * TODO: add javadoc.
 */
public class GeneratorV2 {
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

					Path javadocDir = Paths.get(rootDir, module, "target/site/apidocs", packageName.replaceAll("\\.", "/"));

					Map<String, String> mapping = getFieldJavadocMapping(javadocDir.resolve(javadocDir.resolve(matcher.group(1) + ".html")));
				}
			}
		}
	}

	private static Map<String, String> getFieldJavadocMapping(Path file) throws IOException {
		Document d = Jsoup.parse(file.toFile(), StandardCharsets.UTF_8.name());

		Element body = d.body();

		//System.out.println(body);
		Elements memberSummary = body.getElementsByAttribute("summary");

		List<Tuple2<String, String>> collect = memberSummary.stream()
			.flatMap(element -> element.children().stream())
			.filter(element -> element.tagName().equals("tbody"))
			.flatMap(element -> element.children().stream())
			.filter(element -> element.tagName().equals("tr"))
			.filter(element -> element.classNames().contains("altColor") || element.classNames().contains("rowColor"))
			.flatMap(element -> element.children().stream())
			.filter(element -> element.tagName().equals("td") && element.classNames().contains("colLast"))
			.map(GeneratorV2::extractInfo)
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
}
