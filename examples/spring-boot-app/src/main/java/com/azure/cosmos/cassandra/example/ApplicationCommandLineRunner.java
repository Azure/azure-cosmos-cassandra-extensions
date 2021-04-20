// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.azure.cosmos.cassandra.example.data.Person;
import com.azure.cosmos.cassandra.example.data.ReactivePersonRepository;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Runs the application with output written the standard output device.
 */
@SpringBootApplication
public class ApplicationCommandLineRunner implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationCommandLineRunner.class);
    private final ReactivePersonRepository reactivePersonRepository;

    /**
     * Initializes a new instance of the {@link ApplicationCommandLineRunner application}.
     *
     * @param reactivePersonRepository a reference to a repository instance containing people.
     */
    public ApplicationCommandLineRunner(@Autowired final ReactivePersonRepository reactivePersonRepository) {
        this.reactivePersonRepository = reactivePersonRepository;
    }

    /**
     * The main {@linkplain ApplicationCommandLineRunner application} entry point.
     *
     * @param args an array of arguments.
     */
    public static void main(final String[] args) {
        SpringApplication.run(ApplicationCommandLineRunner.class, args);
    }

    /**
     * Runs the {@linkplain ApplicationCommandLineRunner application} logic. This method is called by Spring Boot after
     * it instantiates the {@linkplain ApplicationCommandLineRunner application}.
     *
     * @param args a variable argument list.
     */
    @SuppressFBWarnings("DM_EXIT")
    @Override
    public void run(final String... args) {

        try {
            final int iterations = args.length == 0 ? 1 : Integer.parseUnsignedInt(args[0]);
            MetricsSnapshot metricsSnapshot;
            int requestCount = 0;
            int errorCount = 0;

            metricsSnapshot = this.importData();
            errorCount += metricsSnapshot.getErrorCount();
            requestCount += metricsSnapshot.getRequestCount();
            System.out.println("\nImport request metrics: " + metricsSnapshot);

            metricsSnapshot = this.tabulatePeopleYoungerThanEachPerson(iterations);
            errorCount += metricsSnapshot.getErrorCount();
            requestCount += metricsSnapshot.getRequestCount();
            System.out.println("\nTabulation request metrics: " + metricsSnapshot);

            LOG.info("Requests: {}, Errors: {}", requestCount, errorCount);
            System.exit(errorCount == 0 ? 0 : 1);

        } catch (final Throwable error) {
            System.out.print("Application startup failed due to: ");
            error.printStackTrace();
            LOG.error("Application startup failed due to: ", error);
            System.exit(2);
        }
    }

    private MetricsSnapshot importData() throws IOException, URISyntaxException, CsvValidationException {

        // Process each person represented in the data set

        final InputStream stream = ApplicationCommandLineRunner.class
            .getClassLoader()
            .getResourceAsStream("people.csv");

        final CsvRecordReader reader = new CsvRecordReader(stream);
        final Metrics metrics = new Metrics();

        Flux.fromStream(reader.stream())
            .flatMap(
                record -> {
                    final Person person = Person.from(record);
                    return this.reactivePersonRepository.insert(person)
                        .doOnSuccess(insertedPerson -> metrics.incrementRequests())
                        .doOnError(error ->
                            LOG.error("Request to insert {} failed due to:", person, metrics.addError(error)));
                })
            .parallel()
            .runOn(Schedulers.parallel())
            .doOnError(error -> LOG.error("Insert failed due to: ", error))
            .sequential()
            .reduce(0, (subtotal, person) -> subtotal + 1)
            .blockOptional();

        return metrics.snapshot();
    }

    @SuppressWarnings({ "Convert2MethodRef", "LocalCanBeFinal", "unchecked" })
    private MetricsSnapshot tabulatePeopleYoungerThanEachPerson(final int iterations) {

        final Metrics metrics = new Metrics();

        Flux.range(1, iterations).concatMap(iteration -> {

            final CsvRecordReader reader;

            try {
                final InputStream stream = ApplicationCommandLineRunner.class
                    .getClassLoader()
                    .getResourceAsStream("people.csv");
                reader = new CsvRecordReader(stream);
            } catch (Throwable error) {
                return Mono.just(error);
            }

            return Flux.fromStream(reader.stream()).flatMap(record -> {

                final Person elder = Person.from(record);
                final LocalDateTime date = elder.getId().getBirthDate();
                final Flux<Person> youngerPeople = this.reactivePersonRepository.findByIdBirthDateGreaterThan(date);

                return youngerPeople.collectSortedList()
                    .map(sortedList -> Tuples.of(elder, sortedList))
                    .doOnSuccess(person -> metrics.incrementRequests())
                    .doOnError(error -> LOG.error("[Iteration {}] failed to tabulate results for {} due to:",
                        iteration,
                        elder,
                        metrics.addError(error)));

            }).parallel().runOn(Schedulers.parallel()).sequential().collect(

                () ->
                    new ConcurrentHashMap<Person, List<Person>>(),

                (concurrentMap, elderYoungerPeople) -> concurrentMap.compute(
                    elderYoungerPeople.getT1(),
                    (elder, youngerPeople) -> elderYoungerPeople.getT2())

            ).map(concurrentMap -> Tuples.of(iteration, Collections.unmodifiableMap(concurrentMap)));

        }).cast(Tuple2.class).reduce(0, (subtotal, result) -> {

            final int iteration = (int) result.getT1();
            final Map<Person, List<Person>> youngerPeople = (Map<Person, List<Person>>) result.getT2();

            System.out.println("----------------------------");
            System.out.println("Y O U N G E R  P E O P L E");
            System.out.println("----------------------------");

            System.out.printf("Iteration: %03d, people: %d%n%n", iteration, youngerPeople.size());
            int i = 0;

            for (final Map.Entry<Person, List<Person>> entry : youngerPeople.entrySet()) {

                System.out.printf("Elder-%03d. %s%n", ++i, entry.getKey());
                int j = 0;

                for (final Person younger : entry.getValue()) {
                    System.out.printf("  Younger-%03d. %s%n", ++j, younger);
                }
            }
            return subtotal + 1;

        }).block();

        return metrics.snapshot();
    }

    private static class CsvRecordReader implements AutoCloseable, Iterable<Map<String, String>> {

        private final String[] header;
        private final CSVReader reader;

        CsvRecordReader(final InputStream inputStream) throws IOException {
            this.reader = new CSVReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            this.header = this.reader.readNextSilently();
        }

        @Override
        public void close() {
            try {
                this.reader.close();
            } catch (final IOException suppressed) {
                final IllegalStateException error = new IllegalStateException(suppressed.getMessage(), suppressed);
                error.addSuppressed(suppressed);
                throw error;
            }
        }

        @Override
        @NonNull
        public Iterator<Map<String, String>> iterator() {
            return new CsvIterator(this.reader, this.header);
        }

        public Stream<Map<String, String>> stream() {
            return StreamSupport.stream(this.spliterator(), false);
        }

        private static final class CsvIterator implements Iterator<Map<String, String>> {

            private final String[] header;
            private final Iterator<String[]> iterator;

            private CsvIterator(final CSVReader reader, final String[] header) {
                this.iterator = reader.iterator();
                this.header = header;
            }

            @Override
            public boolean hasNext() {
                return this.iterator.hasNext();
            }

            @Override
            public Map<String, String> next() {

                final Map<String, String> record = new HashMap<>();
                final String[] row = this.iterator.next();

                for (int i = 0; i < this.header.length; i++) {
                    record.put(this.header[i], row[i]);
                }

                return record;
            }
        }
    }

    private static class Metrics {

        private final List<Throwable> errors;
        private volatile int requests;

        Metrics() {
            this.errors = new ArrayList<>();
            this.requests = 0;
        }

        public synchronized Throwable addError(final Throwable error) {
            this.errors.add(error);
            return error;
        }

        @SuppressWarnings("UnusedReturnValue")
        public synchronized int incrementRequests() {
            return ++this.requests;
        }

        public synchronized MetricsSnapshot snapshot() {
            return new MetricsSnapshot(this);
        }

        @Override
        public String toString() {
            final MetricsSnapshot snapshot = this.snapshot();
            return snapshot.toString();
        }
    }

    private static final class MetricsSnapshot {

        private final List<Throwable> errors;
        private final int requests;

        @SuppressWarnings("Java9CollectionFactory")
        private MetricsSnapshot(final Metrics that) {
            this.errors = Collections.unmodifiableList(new ArrayList<>(that.errors));
            this.requests = that.requests;
        }

        public int getErrorCount() {
            return this.errors.size();
        }

        public List<Throwable> getErrors() {
            return this.errors;
        }

        public int getRequestCount() {
            return this.requests;
        }

        @Override
        public String toString() {
            return
                "{requests:" + this.requests + ",errors:{count:" + this.errors.size() + ",list:" + this.errors + "}}";
        }
    }
}
