/*
 * Copyright 2023 Swisscom (Schweiz) AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.swisscom.kafka.schemaregistry.yang;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.io.File;
import java.net.URL;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yangcentral.yangkit.comparator.CompatibilityRules;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.model.api.stmt.Import;
import org.yangcentral.yangkit.model.api.stmt.Module;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.register.YangStatementImplRegister;
import org.yangcentral.yangkit.register.YangStatementRegister;

public class YangSchemaProvider extends AbstractSchemaProvider {

  public static final String YANG_COMPARATOR_RULES_CONFIG = "yang.comparator.rules.path";
  private static final String YANG_COMPARATOR_DEFAULT_RULES = "default-rules.xml";
  private static final Logger log = LoggerFactory.getLogger(YangSchemaProvider.class);

  public YangSchemaProvider() {
    URL inputStream = YangSchema.class.getClassLoader().getResource(YANG_COMPARATOR_DEFAULT_RULES);
    try {
      SAXReader reader = new SAXReader();
      Document document = reader.read(inputStream);
      CompatibilityRules.getInstance().deserialize(document);
    } catch (Exception e) {
      throw new IllegalArgumentException("Couldn't load comparator rules", e);
    }
    YangStatementImplRegister.registerImpl();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    Document document;
    try {
      if (configs.containsKey(YangSchemaProvider.YANG_COMPARATOR_RULES_CONFIG)) {
        String rulesPath = (String) configs.get(YangSchemaProvider.YANG_COMPARATOR_RULES_CONFIG);
        SAXReader reader = new SAXReader();
        document = reader.read(new File(rulesPath));
      } else {
        URL rulesStream =
            YangSchema.class.getClassLoader().getResource(YANG_COMPARATOR_DEFAULT_RULES);
        SAXReader reader = new SAXReader();
        document = reader.read(rulesStream);
      }
      CompatibilityRules.getInstance().deserialize(document);
    } catch (Exception e) {
      throw new IllegalArgumentException("Couldn't load comparator rules", e);
    }
  }

  @Override
  public String schemaType() {
    return YangSchema.TYPE;
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
    log.debug("parseSchemaOrElseThrow schema: {}, new: {}", schema, isNew);
    YangSchemaContext context = YangStatementRegister.getInstance().getSchemeContextInstance();
    Map<String, String> resolvedReferences = resolveReferences(schema);

    try {
      // Parse first resolved references
      for (Map.Entry<String, String> entry : resolvedReferences.entrySet()) {
        YangSchemaUtils.parseYangString(entry.getKey(), entry.getValue(), context);
      }
      YangSchemaUtils.parseSchema(schema, context);
      context.validate();
      Module rootModule = context.getModules().get(context.getModules().size() - 1);
      for (Import imported : rootModule.getImports()) {
        // AH: do we need to resolve imports recursively?! Assuming this check was done on each one
        if (!resolvedReferences.containsKey(imported.getArgStr())) {
          throw new IllegalArgumentException("Unresolved import: " + imported);
        }
      }
      YangSchema yangSchema =
          new YangSchema(
              schema.getSchema(), context, rootModule, schema.getReferences(), resolvedReferences);
      return yangSchema;
    } catch (YangParserException e) {
      log.error("Error parsing Yang Schema", e);
      throw new IllegalArgumentException("Invalid Yang " + schema.getSchema(), e);
    } catch (Exception e) {
      log.error("Error parsing Yang Schema", e);
      throw e;
    }
  }
}
