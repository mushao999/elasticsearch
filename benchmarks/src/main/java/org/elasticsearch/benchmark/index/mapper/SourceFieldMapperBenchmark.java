/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.mapper;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.SourceFieldMapper.RECOVERY_SOURCE_NAME;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class SourceFieldMapperBenchmark {
    private DocumentParserContext context;
    private CheckedBiFunction<BytesReference, XContentType, BytesReference, IOException> parserFilter;
    private Function<Map<String, ?>, Map<String, Object>> filter;
    private final boolean enabled = true;
    @Param({ "tiny", "short", "one_4k_field", "one_4m_field" })
    private String source;
    @Param({ "message" })
    private String includes;
    @Param({ "" })
    private String excludes;

    @Setup
    public void setup() throws IOException {
        switch (source) {
            case "tiny":
                context = new BenchmarkDocumentParserContext(new BytesArray("{\"message\": \"short\"}"));
                break;
            case "short":
                context = new BenchmarkDocumentParserContext(read300BytesExample());
                break;
            case "one_4k_field":
                context = new BenchmarkDocumentParserContext(buildBigExample("huge".repeat(1024)));
                break;
            case "one_4m_field":
                context = new BenchmarkDocumentParserContext(buildBigExample("huge".repeat(1024 * 1024)));
                break;
            default:
                throw new IllegalArgumentException("Unknown source [" + source + "]");
        }
        String[] includesArrays = Strings.splitStringByCommaToArray(includes);
        String[] excludesArrays = Strings.splitStringByCommaToArray(excludes);
        filter = XContentMapValues.filter(includesArrays, excludesArrays);
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withFiltering(
            Set.of(includesArrays),
            Set.of(excludesArrays)
        );
        parserFilter = (sourceBytes, contentType) -> {
            BytesStreamOutput streamOutput = new BytesStreamOutput(Math.min(1024, sourceBytes.length()));
            XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), streamOutput);
            XContentParser parser = XContentType.JSON.xContent().createParser(parserConfig, sourceBytes.streamInput());
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        };
    }

    private BytesReference read300BytesExample() throws IOException {
        return Streams.readFully(SourceFieldMapperBenchmark.class.getResourceAsStream("300b_example.json"));
    }

    private BytesReference buildBigExample(String extraText) throws IOException {
        String bigger = read300BytesExample().utf8ToString();
        bigger = "{\"huge\": \"" + extraText + "\"," + bigger.substring(1);
        return new BytesArray(bigger);
    }

    @Benchmark
    public void preParseUsingMapFilter() throws IOException {
        preParse(context, this::applyFilters);
    }

    @Benchmark
    public void preParseUsingParserFilter() throws IOException {
        preParse(context, parserFilter);
    }

    /**
     * same process to {@link SourceFieldMapper#preParse(DocumentParserContext)}
     * @param context DocumentParserContext
     * @param sourceFilter the source filter which the benchmark run with
     * @throws IOException
     */
    public void preParse(
        DocumentParserContext context,
        CheckedBiFunction<BytesReference, XContentType, BytesReference, IOException> sourceFilter
    ) throws IOException {
        BytesReference originalSource = context.sourceToParse().source();
        XContentType contentType = context.sourceToParse().getXContentType();
        final BytesReference adaptedSource = sourceFilter.apply(originalSource, contentType);

        if (adaptedSource != null) {
            final BytesRef ref = adaptedSource.toBytesRef();
            context.doc().add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        }

        if (originalSource != null && adaptedSource != originalSource) {
            // if we omitted source or modified it we add the _recovery_source to ensure we have it for ops based recovery
            BytesRef ref = originalSource.toBytesRef();
            context.doc().add(new StoredField(RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            context.doc().add(new NumericDocValuesField(RECOVERY_SOURCE_NAME, 1));
        }
    }

    /**
     * original source filter logic for {@link SourceFieldMapper} before optimization
     * @param originalSource original source BytesReference
     * @param contentType content type
     * @return filtered source BytesReference
     * @throws IOException -
     */
    public BytesReference applyFilters(BytesReference originalSource, XContentType contentType) throws IOException {
        if (enabled && originalSource != null) {
            // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
            if (filter != null) {
                // we don't update the context source if we filter, we want to keep it as is...
                Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(originalSource, true, contentType);
                Map<String, Object> filteredSource = filter.apply(mapTuple.v2());
                BytesStreamOutput bStream = new BytesStreamOutput();
                XContentType actualContentType = mapTuple.v1();
                XContentBuilder builder = XContentFactory.contentBuilder(actualContentType, bStream).map(filteredSource);
                builder.close();
                return bStream.bytes();
            } else {
                return originalSource;
            }
        } else {
            return null;
        }
    }

    private static class BenchmarkDocumentParserContext extends DocumentParserContext {
        private final LuceneDocument document = new LuceneDocument();

        protected BenchmarkDocumentParserContext(BytesReference sourceBytes) {
            super(MappingLookup.EMPTY, null, null, null, new SourceToParse("source_mapper_benchmark", "1", sourceBytes, XContentType.JSON));
        }

        @Override
        public LuceneDocument doc() {
            return document;
        }

        @Override
        public ContentPath path() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<LuceneDocument> nonRootDocuments() {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentParser parser() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LuceneDocument rootDoc() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void addDoc(LuceneDocument doc) {
            throw new UnsupportedOperationException();
        }
    }
}
