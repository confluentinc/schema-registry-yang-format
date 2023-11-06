package io.confluent.kafka.schemaregistry.yang;

public class TestSchemas {
  static final String SIMPLE_SCHEMA =
      "module a-module {\n"
          + "  yang-version 1.1;\n"
          + "  namespace \"urn:example:a-module\";\n"
          + "  prefix a;\n"
          + "\n"
          + "  organization\n"
          + "    \"Test Organization\";\n"
          + "  contact\n"
          + "    \"Ahmed Elhassany <mailto:ahmed.elhassany@swisscom.com>\";\n"
          + "  description\n"
          + "    \"Simple Test module with one typedef and a container of one leaf\";\n"
          + "  reference\n"
          + "    \"testing\";\n"
          + "\n"
          + "  revision 2023-02-01 {\n"
          + "    reference\n"
          + "      \"first revision\";\n"
          + "  }\n"
          + "\n"
          + "  typedef aType {\n"
          + "    type int8;\n"
          + "    description\n"
          + "      \"Simple int8 type for testing\";\n"
          + "  }\n"
          + "\n"
          + "  container x {\n"
          + "    description\n"
          + "      \"Test container\";\n"
          + "    leaf aLeaf {\n"
          + "      type aType;\n"
          + "      description\n"
          + "        \"Example leaf\";\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  static final String SIMPLE_SCHEMA_REORDERED =
      "module a-module {\n"
          + "  yang-version 1.1;\n"
          + "  namespace \"urn:example:a-module\";\n"
          + "  prefix a;\n"
          + "\n"
          + "  organization\n"
          + "    \"Test Organization\";\n"
          + "  contact\n"
          + "    \"Ahmed Elhassany <mailto:ahmed.elhassany@swisscom.com>\";\n"
          + "  description\n"
          + "    \"Simple Test module with one typedef and a container of one leaf\";\n"
          + "  reference\n"
          + "    \"testing\";\n"
          + "\n"
          + "  revision 2023-02-01 {\n"
          + "    reference\n"
          + "      \"first revision\";\n"
          + "  }\n"
          + "\n"
          + "  container x {\n"
          + "    description\n"
          + "      \"Test container\";\n"
          + "    leaf aLeaf {\n"
          + "      type aType;\n"
          + "      description\n"
          + "        \"Example leaf\";\n"
          + "    }\n"
          + "  }\n"
          + "  typedef aType {\n"
          + "    type int8;\n"
          + "    description\n"
          + "      \"Simple int8 type for testing\";\n"
          + "  }\n"
          + "}\n";
}
