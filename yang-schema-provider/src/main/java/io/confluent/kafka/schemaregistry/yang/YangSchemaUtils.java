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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import org.yangcentral.yangkit.base.YangElement;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.model.api.stmt.Module;
import org.yangcentral.yangkit.model.api.stmt.YangStatement;
import org.yangcentral.yangkit.parser.YangParser;
import org.yangcentral.yangkit.parser.YangParserEnv;
import org.yangcentral.yangkit.parser.YangParserException;

import java.util.List;

public class YangSchemaUtils {

  public static void parseYangString(String name, String schemaString, YangSchemaContext context)
      throws YangParserException {
    YangParser yangParser = new YangParser();
    YangParserEnv yangParserEnv = new YangParserEnv();
    yangParserEnv.setYangStr(schemaString);
    yangParserEnv.setFilename(name);
    yangParserEnv.setCurPos(0);
    List<YangElement> elementList = yangParser.parseYang(schemaString, yangParserEnv);
    // Add the yang module to the context;
    for (YangElement element : elementList) {
      if (element instanceof YangStatement) {
        context.addModule((Module) element);
      }
    }
    String moduleName = name;
    if (context.getModules().size() > 0) {
      moduleName = context.getModules().get(0).getModuleId().getModuleName();
    }
    context.getParseResult().put(moduleName, elementList);
  }

  public static void parseSchema(Schema schema, YangSchemaContext context)
      throws YangParserException {
    parseYangString(schema.getSubject(), schema.getSchema(), context);
  }
}
