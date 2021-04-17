// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.azure.cosmos.cassandra.example.data.Person;
import com.azure.cosmos.cassandra.example.data.PersonId;
import com.azure.cosmos.cassandra.example.data.PersonRepository;
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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Runs the application with output written the standard output device.
 */
@SpringBootApplication
public class ApplicationCommandLineRunner implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationCommandLineRunner.class);

    private final PersonRepository personRepository;
    private final ReactivePersonRepository reactivePersonRepository;

    /**
     * Initializes a new instance of the {@link ApplicationCommandLineRunner application}.
     *
     * @param personRepository a reference to a repository instance containing people.
     */
    public ApplicationCommandLineRunner(
        @Autowired final PersonRepository personRepository,
        @Autowired final ReactivePersonRepository reactivePersonRepository) {

        this.personRepository = personRepository;
        this.reactivePersonRepository = reactivePersonRepository;
    }

    /**
     * The main {@linkplain ApplicationCommandLineRunner application} entry point.
     *
     * @param args an array of arguments.
     */
    public static void main(final String[] args) {
        SpringApplication.run(ApplicationCommandLineRunner.class);
    }

    /**
     * Runs the {@linkplain ApplicationCommandLineRunner application} logic. This method is called by Spring Boot after
     * it instantiates the {@linkplain ApplicationCommandLineRunner application}.
     *
     * @param args a variable argument list.
     */
    @SuppressWarnings("unchecked")
    @SuppressFBWarnings("DM_EXIT")
    @Override
    public void run(final String... args) {

        try {
            //            this.importData();
            //            this.tabulatePeopleWithSameLastName();
            //            this.tabulatePeopleWithSameFirstName();
            //            this.tabulatePeopleWithSameOccupation();
            //            this.tabulateYoungerPeopleThanEachPerson();

            this.reactivelyImportData();
            this.reactivelyTabulatePeopleYoungerThanEachPerson(1);

            System.exit(0);

        } catch (final Throwable error) {
            System.out.print("Application failed due to: ");
            error.printStackTrace();
            System.exit(1);
        }
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void importData() throws IOException, URISyntaxException, CsvValidationException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CsvRecordReader reader = new CsvRecordReader(path)) {

            final Set<String> firstNames = new HashSet<>();

            for (Map<String, String> row : reader) {

                final Person person = new Person(
                    new PersonId(
                        row.get("first_name"),
                        LocalDateTime.parse(row.get("birth_date")),
                        UUID.fromString(row.get("uuid"))),
                    row.get("last_name"),
                    row.get("occupation"));

                this.personRepository.insert(person);
            }
        }
    }

    private void reactivelyImportData() throws IOException, URISyntaxException, CsvValidationException {

        // Process each person represented in the data set

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());
        final CsvRecordReader reader = new CsvRecordReader(path);

        final Optional<Integer> recordCount = Flux.fromStream(reader.stream()).flatMap(
            record -> {
                final Person person = new Person(record);
                return this.reactivePersonRepository
                    .insert(person)
                    .doOnError(error -> LOG.error("Failed to insert {} due to:", person, error));
            })
            .parallel().runOn(Schedulers.parallel())
            .sequential().reduce(0, (subtotal, person) -> subtotal + 1).blockOptional();

        System.out.println("Imported " + recordCount.orElse(0) + " person records from " + path);
    }

    @SuppressWarnings({ "Convert2MethodRef", "LocalCanBeFinal", "unchecked" })
    private void reactivelyTabulatePeopleYoungerThanEachPerson(final int iterations) {

        CompletableFuture<Void> future = new CompletableFuture<>();
        Mono<?> complete = Mono.fromFuture(future);

        Flux.range(1, iterations).concatMap(iteration -> {

            final CsvRecordReader reader;

            try {
                final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());
                reader = new CsvRecordReader(path);
            } catch (Throwable error) {
                return Mono.just(error);
            }

            return Flux.fromStream(reader.stream()).flatMap(record -> {

                final Person elder = new Person(record);
                final LocalDateTime date = elder.getId().getBirthDate();
                final Flux<Person> youngerPeople = this.reactivePersonRepository.findByIdBirthDateGreaterThan(date);

                return youngerPeople.collectSortedList()
                    .map(sortedList -> Tuples.of(elder, sortedList))
                    .doOnError(error -> LOG.error("[Iteration {}]failed to tabulate results for {} due to:",
                        iteration,
                        elder,
                        error));

            }).parallel().runOn(Schedulers.parallel()).sequential().collect(

                () ->
                    new ConcurrentHashMap<Person, List<Person>>(),

                (concurrentMap, elderYoungerPeople) -> concurrentMap.compute(
                    (Person) elderYoungerPeople.getT1(),
                    (elder, youngerPeople) -> (List<Person>) elderYoungerPeople.getT2())

            ).map(concurrentMap -> Tuples.of(iteration, Collections.unmodifiableMap(concurrentMap)));

        }).subscribe(

            result -> {

                final Tuple2<Integer, Map<Person, List<Person>>> item = (Tuple2<Integer, Map<Person, List<Person>>>) result;
                final int iteration = item.getT1();
                final Map<Person, List<Person>> youngerPeople = item.getT2();

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
            },
            null, () -> future.complete(null));

        complete.block();
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void tabulatePeopleWithSameFirstName() throws CsvValidationException, IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CsvRecordReader reader = new CsvRecordReader(path)) {

            final Set<String> firstNames = new HashSet<>();

            for (Map<String, String> row : reader) {

                final String firstName = row.get("first_name");

                if (firstNames.add(firstName)) {

                    System.out.println("People with first name: " + firstName);

                    for (final Person person : this.personRepository.findByIdFirstName(firstName)) {
                        System.out.println("  " + person);
                    }
                }
            }
        }
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void tabulatePeopleWithSameLastName() throws CsvValidationException, IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CsvRecordReader reader = new CsvRecordReader(path)) {

            final Set<String> lastNames = new HashSet<>();

            for (Map<String, String> row : reader) {

                final String lastName = row.get("last_name");

                if (lastNames.add(lastName)) {

                    System.out.println("People with last name: " + lastName);

                    for (final Person person : this.personRepository.findByLastName(lastName)) {
                        System.out.println("  " + person);
                    }
                }
            }
        }
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void tabulatePeopleWithSameOccupation() throws IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CsvRecordReader reader = new CsvRecordReader(path)) {

            final Set<String> occupations = new HashSet<>();

            for (Map<String, String> row : reader) {

                final String occupation = row.get("occupation");

                if (occupations.add(occupation)) {

                    System.out.println("People with occupation: " + occupation);

                    for (final Person person : this.personRepository.findByOccupation(occupation)) {
                        System.out.println("  " + person);
                    }
                }
            }
        }
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void tabulateYoungerPeopleThanEachPerson() throws IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CsvRecordReader reader = new CsvRecordReader(path)) {

            for (Map<String, String> row : reader) {

                final Person person = new Person(
                    new PersonId(
                        row.get("first_name"),
                        LocalDateTime.parse(row.get("birth_date")),
                        UUID.fromString(row.get("uuid"))),
                    row.get("last_name"),
                    row.get("occupation"));

                final LocalDateTime dateTime = person.getId().getBirthDate();
                final List<Person> youngerPeople = this.personRepository.findByIdBirthDateGreaterThan(dateTime);

                System.out.println("People younger than: " + person);

                for (final Person youngerPerson : youngerPeople) {
                    System.out.println("  " + youngerPerson);
                }
            }
        }
    }

    private static class CsvRecordReader implements AutoCloseable, Iterable<Map<String, String>> {

        private final String[] header;
        private final CSVReader reader;

        public CsvRecordReader(final Path path) throws IOException {
            this.reader = new CSVReader(Files.newBufferedReader(path));
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

        private static class CsvIterator implements Iterator<Map<String, String>> {

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

        public Metrics() {
            this.errors = new ArrayList<>();
            this.requests = 0;
        }

        public synchronized void addError(final Throwable error) {
            this.errors.add(error);
        }

        public synchronized void incrementRequests() {
            ++this.requests;
        }

        public synchronized MetricsSnapshot snapshot() {
            return new MetricsSnapshot(this);
        }

        @Override
        public String toString() {

            final MetricsSnapshot snapshot = this.snapshot();

            return "{errors: {count:" + snapshot.errors.size() + ", list:" + snapshot.errors + "},requests:"
                + snapshot.requests + "}";
        }
    }

    private static class MetricsSnapshot {

        private final List<Throwable> errors;
        private final int requests;

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

        public int getRequests() {
            return this.requests;
        }
    }
}
