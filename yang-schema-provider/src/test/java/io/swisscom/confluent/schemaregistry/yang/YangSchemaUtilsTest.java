package com.swisscom.kafka.schemaregistry.yang;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.junit.Before;
import org.junit.Test;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.register.YangStatementImplRegister;
import org.yangcentral.yangkit.register.YangStatementRegister;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class YangSchemaUtilsTest {

  static final String RefYangSchema =
      "module ref {\n"
          + "  yang-version \"1.1\";\n"
          + "  namespace \"urn:example:schema:test:ref\";\n"
          + "  prefix \"ref\";\n"
          + "  revision \"2023-02-06\";\n"
          + "  typedef aType {\n"
          + "    type \"int8\";\n"
          + "  }\n"
          + "}\n";

  static final String RootYangSchema =
      "module root {\n"
          + "  yang-version \"1.1\";\n"
          + "  namespace \"urn:example:schema:test:root\";\n"
          + "  prefix \"root\";\n"
          + "  import ref {\n"
          + "    prefix \"ref\";\n"
          + "    revision-date \"2023-02-06\";\n"
          + "  }\n"
          + "  revision \"2023-02-06\";\n"
          + "  container root {\n"
          + "    leaf testLeaf {\n"
          + "      type \"ref:aType\";\n"
          + "      description\n"
          + "        \"Example leaf\";\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  public static Map<String, String> getYangSchemaWithDependencies() {
    Map<String, String> schemas = new HashMap<>();
    schemas.put("ref", RefYangSchema);
    schemas.put("root", RootYangSchema);
    return schemas;
  }

  @Before
  public void setUp() {
    YangStatementImplRegister.registerImpl();
  }

  @Test
  public void testParseValidSchema() throws YangParserException {
    YangSchemaContext context = YangStatementRegister.getInstance().getSchemeContextInstance();
    YangSchemaUtils.parseYangString("a-module", TestSchemas.SIMPLE_SCHEMA, context);
    assertEquals(1, context.getModules().size());
    assertTrue(context.getModule("a-module", "2023-02-01").isPresent());
  }

  @Test(expected = YangParserException.class)
  public void testParseInvalidSchema() throws YangParserException {
    YangSchemaContext context = YangStatementRegister.getInstance().getSchemeContextInstance();
    YangSchemaUtils.parseYangString("a-module", "module a-module {", context);
  }

  @Test
  public void testHashSchema() {
    Schema schema1 = new Schema("subjectA", 1, 1, new SchemaString(TestSchemas.SIMPLE_SCHEMA));
    Schema schema2 =
        new Schema("subjectA", 1, 1, new SchemaString(TestSchemas.SIMPLE_SCHEMA));
    Schema schema3 =
        new Schema("subjectA", 1, 1, new SchemaString(TestSchemas.SIMPLE_SCHEMA_REORDERED));

    assertEquals(schema1.hashCode(), schema2.hashCode());
    assertNotEquals(schema1.hashCode(), schema3.hashCode());
  }
}
