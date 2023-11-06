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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yangcentral.yangkit.common.api.validate.ValidatorResult;
import org.yangcentral.yangkit.comparator.CompareType;
import org.yangcentral.yangkit.comparator.CompatibilityRule;
import org.yangcentral.yangkit.comparator.YangComparator;
import org.yangcentral.yangkit.comparator.YangCompareResult;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.model.api.stmt.Module;

public class YangSchema implements ParsedSchema {
  private static final Logger log = LoggerFactory.getLogger(YangSchema.class);

  public static final String TYPE = "YANG";

  private final String schemaString;
  private final Module module;
  private final YangSchemaContext context;
  private final List<SchemaReference> references;
  private final Map<String, String> resolvedReferences;
  private final Metadata metadata;
  private final Integer version;
  private final RuleSet ruleSet;

  private static final int NO_HASHCODE = Integer.MIN_VALUE;
  private transient int hashCode = NO_HASHCODE;

  public YangSchema(
      String schemaString,
      Integer version,
      YangSchemaContext context,
      Module module,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Metadata metadata,
      RuleSet ruleSet) {
    this.schemaString = schemaString;
    this.version = version;

    this.context = context;
    this.module = module;
    this.references = Collections.unmodifiableList(references);
    this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
    this.metadata = metadata;
    this.ruleSet = ruleSet;
  }

  public YangSchema(
      String schemaString,
      YangSchemaContext context,
      Module module,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences) {
    this(schemaString, null, context, module, references, resolvedReferences, null, null);
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    return this.module.getModuleId().getModuleName();
  }

  @Override
  public String canonicalString() {
    return this.schemaString;
  }

  @Override
  public Integer version() {
    return this.version;
  }

  @Override
  public List<SchemaReference> references() {
    return this.references;
  }

  @Override
  public Metadata metadata() {
    return this.metadata;
  }

  @Override
  public RuleSet ruleSet() {
    return this.ruleSet;
  }

  @Override
  public ParsedSchema copy() {
    return new YangSchema(
        this.schemaString,
        this.version,
        this.context,
        this.module,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet);
  }

  @Override
  public ParsedSchema copy(Integer version) {
    return new YangSchema(
        this.schemaString,
        version,
        this.context,
        this.module,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet);
  }

  @Override
  public ParsedSchema copy(Metadata metadata, RuleSet ruleSet) {
    return new YangSchema(
        this.schemaString,
        this.version,
        this.context,
        this.module,
        this.references,
        this.resolvedReferences,
        metadata,
        this.ruleSet);
  }

  @Override
  public ParsedSchema copy(
      Map<SchemaEntity, Set<String>> tagsToAdd, Map<SchemaEntity, Set<String>> tagsToRemove) {
    throw new UnsupportedOperationException("Tag modifications is not implemented for YANG Schema");
  }

  public YangSchemaContext yangSchemaContext() {
    return this.context;
  }

  private static void writeDom4jDoc(org.dom4j.Document doc, OutputStream outputStream) {
    OutputFormat format = OutputFormat.createPrettyPrint();
    try {
      XMLWriter writer = new XMLWriter(outputStream, format);
      writer.write(doc);
      writer.close();
      outputStream.close();
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }

  @Override
  public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
    log.debug("Checking if schema is backward compatible: {} and {}", this, previousSchema);
    if (!(previousSchema instanceof YangSchema)) {
      return Collections.singletonList("Incompatible schema types");
    }
    YangSchema previousYangSchema = (YangSchema) previousSchema;
    YangComparator comparator =
        new YangComparator(this.context, previousYangSchema.yangSchemaContext());

    try {
      CompareType compareType = CompareType.COMPATIBLE_CHECK;
      List<YangCompareResult> compareResults = comparator.compare(compareType, null);
      boolean nonBackwardCompatible =
          compareResults.stream()
              .map(x -> x.getCompatibilityInfo().getCompatibility())
              .anyMatch(x -> x == CompatibilityRule.Compatibility.NBC);
      List<String> ret = new ArrayList<>();
      if (nonBackwardCompatible) {
        boolean needCompatible = true;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        writeDom4jDoc(
            comparator.outputXmlCompareResult(compareResults, needCompatible, compareType),
            byteArrayOutputStream);
        ret.add(byteArrayOutputStream.toString(Charset.defaultCharset().name()));
      }
      return ret;
    } catch (Exception e) {
      log.error("Yang Schema Comparator exception", e);
      return Collections.singletonList("Incompatible schema types");
    }
  }

  @Override
  public YangSchema normalize() {
    return this;
  }

  @Override
  public void validate() {
    ValidatorResult result = this.context.validate();
    if (!result.isOk()) {
      throw new IllegalArgumentException(
          "Invalid YANG schema:\n"
              + result.getRecords().stream()
                  .map(Object::toString)
                  .collect(Collectors.joining("\n")));
    }
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASHCODE) {
      hashCode =
          Objects.hash(
              this.module.getModuleId().getModuleName(),
              this.module.getModuleId().getRevision(),
              this.module.getSubElements(),
              references,
              version(),
              metadata,
              ruleSet());
    }
    return hashCode;
  }

  @Override
  public Module rawSchema() {
    return this.module;
  }
}
