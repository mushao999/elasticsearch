/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public final class BackgroundIndexer implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(BackgroundIndexer.class);

    final Thread[] writers;
    final Client client;
    final CountDownLatch stopLatch;
    final Collection<Exception> failures = new ArrayList<>();
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong idGenerator = new AtomicLong();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean hasBudget = new AtomicBoolean(false); // when set to true, writers will acquire writes from a semaphore
    final Semaphore availableBudget = new Semaphore(0);
    final boolean useAutoGeneratedIDs;
    private final Set<String> ids = ConcurrentCollections.newConcurrentSet();
    private volatile Consumer<Exception> failureAssertion = null;

    volatile int minFieldSize = 10;
    volatile int maxFieldSize = 140;

    /**
     * Start indexing in the background using a random number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param index     index name to index into
     * @param client    client to use
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public BackgroundIndexer(String index, Client client, int numOfDocs) {
        this(index, client, numOfDocs, RandomizedTest.scaledRandomIntBetween(2, 5));
    }

    /**
     * Start indexing in the background using a given number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param index       index name to index into
     * @param client      client to use
     * @param numOfDocs   number of document to index before pausing. Set to -1 to have no limit.
     * @param writerCount number of indexing threads to use
     */
    public BackgroundIndexer(String index, Client client, int numOfDocs, final int writerCount) {
        this(index, client, numOfDocs, writerCount, true, null);
    }

    /**
     * Start indexing in the background using a given number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param index       index name to index into
     * @param client      client to use
     * @param numOfDocs   number of document to index before pausing. Set to -1 to have no limit.
     * @param writerCount number of indexing threads to use
     * @param autoStart   set to true to start indexing as soon as all threads have been created.
     * @param random      random instance to use
     */
    public BackgroundIndexer(
        final String index,
        final Client client,
        final int numOfDocs,
        final int writerCount,
        boolean autoStart,
        Random random
    ) {

        if (random == null) {
            random = RandomizedTest.getRandom();
        }
        this.client = client;
        useAutoGeneratedIDs = random.nextBoolean();
        writers = new Thread[writerCount];
        stopLatch = new CountDownLatch(writers.length);
        logger.info("--> creating {} indexing threads (auto start: [{}], numOfDocs: [{}])", writerCount, autoStart, numOfDocs);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            final boolean batch = random.nextBoolean();
            final Random threadRandom = new Random(random.nextLong());
            writers[i] = new Thread() {
                @Override
                public void run() {
                    long id = -1;
                    try {
                        startLatch.await();
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (stop.get() == false) {
                            if (batch) {
                                int batchSize = threadRandom.nextInt(20) + 1;
                                if (hasBudget.get()) {
                                    // always try to get at least one
                                    batchSize = Math.max(Math.min(batchSize, availableBudget.availablePermits()), 1);
                                    if (availableBudget.tryAcquire(batchSize, 250, TimeUnit.MILLISECONDS) == false) {
                                        // time out -> check if we have to stop.
                                        continue;
                                    }

                                }
                                BulkRequestBuilder bulkRequest = client.prepareBulk().setTimeout(timeout);
                                for (int i = 0; i < batchSize; i++) {
                                    id = idGenerator.incrementAndGet();
                                    if (useAutoGeneratedIDs) {
                                        bulkRequest.add(client.prepareIndex(index).setSource(generateSource(id, threadRandom)));
                                    } else {
                                        bulkRequest.add(
                                            client.prepareIndex(index).setId(Long.toString(id)).setSource(generateSource(id, threadRandom))
                                        );
                                    }
                                }
                                try {
                                    BulkResponse bulkResponse = bulkRequest.get();
                                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                                        if (bulkItemResponse.isFailed() == false) {
                                            boolean add = ids.add(bulkItemResponse.getId());
                                            assert add : "ID: " + bulkItemResponse.getId() + " already used";
                                        } else {
                                            trackFailure(bulkItemResponse.getFailure().getCause());
                                        }
                                    }
                                } catch (Exception e) {
                                    if (ignoreIndexingFailures == false) {
                                        throw e;
                                    }
                                }
                            } else {

                                if (hasBudget.get() && availableBudget.tryAcquire(250, TimeUnit.MILLISECONDS) == false) {
                                    // time out -> check if we have to stop.
                                    continue;
                                }
                                id = idGenerator.incrementAndGet();
                                if (useAutoGeneratedIDs) {
                                    try {
                                        DocWriteResponse indexResponse = client.prepareIndex(index)
                                            .setTimeout(timeout)
                                            .setSource(generateSource(id, threadRandom))
                                            .get();
                                        boolean add = ids.add(indexResponse.getId());
                                        assert add : "ID: " + indexResponse.getId() + " already used";
                                    } catch (Exception e) {
                                        if (ignoreIndexingFailures == false) {
                                            throw e;
                                        }
                                    }
                                } else {
                                    try {
                                        DocWriteResponse indexResponse = client.prepareIndex(index)
                                            .setId(Long.toString(id))
                                            .setTimeout(timeout)
                                            .setSource(generateSource(id, threadRandom))
                                            .get();
                                        boolean add = ids.add(indexResponse.getId());
                                        assert add : "ID: " + indexResponse.getId() + " already used";
                                    } catch (Exception e) {
                                        if (ignoreIndexingFailures == false) {
                                            throw e;
                                        }
                                    }
                                }
                            }
                        }
                        logger.info("**** done indexing thread {}  stop: {} numDocsIndexed: {}", indexerId, stop.get(), ids.size());
                    } catch (Exception e) {
                        trackFailure(e);
                        final long docId = id;
                        logger.warn(() -> format("**** failed indexing thread %s on doc id %s", indexerId, docId), e);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        if (autoStart) {
            start(numOfDocs);
        }
    }

    private void trackFailure(Exception e) {
        synchronized (failures) {
            if (failureAssertion != null) {
                failureAssertion.accept(e);
            } else {
                failures.add(e);
            }
        }
    }

    private XContentBuilder generateSource(long id, Random random) throws IOException {
        int contentLength = RandomNumbers.randomIntBetween(random, minFieldSize, maxFieldSize);
        StringBuilder text = new StringBuilder(contentLength);
        while (text.length() < contentLength) {
            int tokenLength = RandomNumbers.randomIntBetween(random, 1, Math.min(contentLength - text.length(), 10));
            text.append(" ").append(RandomStrings.randomRealisticUnicodeOfCodepointLength(random, tokenLength));
        }
        XContentBuilder builder = XContentFactory.smileBuilder();
        builder.startObject().field("test", "value" + id).field("text", text.toString()).field("id", id).endObject();
        return builder;

    }

    private volatile TimeValue timeout = BulkShardRequest.DEFAULT_TIMEOUT;

    public void setRequestTimeout(TimeValue requestTimeout) {
        this.timeout = requestTimeout;
    }

    private volatile boolean ignoreIndexingFailures;

    public void setIgnoreIndexingFailures(boolean ignoreIndexingFailures) {
        this.ignoreIndexingFailures = ignoreIndexingFailures;
    }

    private void setBudget(int numOfDocs) {
        logger.debug("updating budget to [{}]", numOfDocs);
        if (numOfDocs >= 0) {
            hasBudget.set(true);
            availableBudget.release(numOfDocs);
        } else {
            hasBudget.set(false);
        }

    }

    /**
     * Start indexing
     *
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public void start(int numOfDocs) {
        assert stop.get() == false : "background indexer can not be started after it has stopped";
        setBudget(numOfDocs);
        startLatch.countDown();
    }

    /** Pausing indexing by setting current document limit to 0 */
    public void pauseIndexing() {
        availableBudget.drainPermits();
        setBudget(0);
    }

    /**
     * Continue indexing after it has paused.
     *
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public void continueIndexing(int numOfDocs) {
        setBudget(numOfDocs);
    }

    /** Stop all background threads but don't wait for ongoing indexing operations to finish * */
    public void stop() {
        stop.set(true);
    }

    public void awaitStopped() throws InterruptedException {
        assert stop.get();
        Assert.assertThat("timeout while waiting for indexing threads to stop", stopLatch.await(6, TimeUnit.MINUTES), equalTo(true));
        if (failureAssertion == null) {
            assertNoFailures();
        }
    }

    /** Stop all background threads and wait for ongoing indexing operations to finish * */
    public void stopAndAwaitStopped() throws InterruptedException {
        stop();
        awaitStopped();
    }

    public long totalIndexedDocs() {
        return ids.size();
    }

    public void assertNoFailures() {
        synchronized (failures) {
            Assert.assertThat(failures, emptyIterable());
        }
    }

    /**
     * Set a consumer that can be used to run assertions on failures during indexing. If such a consumer is set then it disables adding
     * failures to {@link #failures}. Should be used if the number of expected failures during indexing could become very large.
     */
    public void setFailureAssertion(Consumer<Exception> failureAssertion) {
        synchronized (failures) {
            this.failureAssertion = failureAssertion;
            boolean success = false;
            try {
                for (Exception failure : failures) {
                    failureAssertion.accept(failure);
                }
                failures.clear();
                success = true;
            } finally {
                if (success == false) {
                    stop();
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        stopAndAwaitStopped();
    }

    public Client getClient() {
        return client;
    }

    /**
     * Returns the ID set of all documents indexed by this indexer run
     */
    public Set<String> getIds() {
        return this.ids;
    }
}
