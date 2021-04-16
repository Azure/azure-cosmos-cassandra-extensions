// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.azure.cosmos.cassandra.example.data.Person;
import com.azure.cosmos.cassandra.example.data.PersonId;
import com.azure.cosmos.cassandra.example.data.PersonRepository;
import com.azure.cosmos.cassandra.example.data.ReactivePersonRepository;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.StreamSupport;

/**
 * Runs the application with output written the standard output device.
 */
@SpringBootApplication
public class ApplicationCommandLineRunner implements CommandLineRunner {

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

            final CompletableFuture<Map<Person, ?>[]>[] iterations =
                (CompletableFuture<Map<Person, ?>[]>[]) Array.newInstance(CompletableFuture.class, 1000);

            for (int i = 0; i < iterations.length; i++) {
                iterations[i] = this.reactivelyTabulateYoungerPeopleThanEachPerson(i);
            }

            CompletableFuture.allOf(iterations).whenComplete((result, error) -> {

                if (error != null) {
                    System.out.println("failed due to " + error);
                    System.exit(1);
                }

                for (int i = 0; i < iterations.length; i++) {

                    if (iterations[i].isCompletedExceptionally()) {
                        continue;
                    }

                    final Map<Person, ?>[] results = Objects.requireNonNull(iterations[i].getNow(null));
                    final Map<Person, List<Person>> youngerPeople = (Map<Person, List<Person>>) results[0];
                    final Map<Person, Metrics> requestMetrics = (Map<Person, Metrics>) results[1];

                    System.out.println("----------------------------");
                    System.out.println("Y O U N G E R  P E O P L E");
                    System.out.println("----------------------------");

                    System.out.printf("Iteration %03d%n%n", i);
                    int j = 0;

                    for (final Map.Entry<Person, List<Person>> entry : youngerPeople.entrySet()) {

                        System.out.printf("Elder-%03d. %s%n", ++j, entry.getKey());
                        int k = 0;

                        for (final Person younger : entry.getValue()) {
                            System.out.printf("  Younger-%03d. %s%n", ++k, younger);
                        }
                    }

                    System.out.println("----------------------------");
                    System.out.println("R E Q U E S T  M E T R I C S");
                    System.out.println("----------------------------");

                    System.out.println("{"
                        + "iteration:" + i
                        + ",requestsSent:" + requestMetrics.size()
                        + ",errorsReceived:" + Flux.fromIterable(requestMetrics.values())
                        .reduce(0, (subtotal, metrics) -> subtotal + metrics.snapshot().getErrorCount()).block()
                        + "}");

                    int number = 0;

                    for (final Map.Entry<Person, Metrics> entry : requestMetrics.entrySet()) {
                        System.out.println(
                            "{number:" + ++number + ",person:" + entry.getKey() + ",metrics:" + entry.getValue() + "}");
                    }
                }

                System.exit(0);
            });

        } catch (final Throwable error) {
            System.out.print("Application failed due to: ");
            error.printStackTrace();
            System.exit(1);
        }
    }

    private Map<String, Integer> getDataDictionary(final CSVReader reader) throws IOException {

        final Map<String, Integer> dataDictionary = new HashMap<>();
        final String[] line = reader.readNextSilently();

        for (int i = 0; i < line.length; i++) {
            dataDictionary.put(line[i], i);
        }
        return dataDictionary;
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void importData() throws IOException, URISyntaxException, CsvValidationException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {

            final Map<String, Integer> dataDictionary = this.getDataDictionary(reader);
            String[] line;

            while ((line = reader.readNext()) != null) {

                final Person person = new Person(
                    new PersonId(
                        line[dataDictionary.get("first_name")],
                        LocalDateTime.parse(line[dataDictionary.get("birth_date")]),
                        UUID.fromString(line[dataDictionary.get("uuid")])),
                    line[dataDictionary.get("last_name")],
                    line[dataDictionary.get("occupation")]);

                this.personRepository.insert(person);
            }
        }
    }

    private void reactivelyImportData() throws IOException, URISyntaxException, CsvValidationException {

        // Setup our CSV Reader, Data dictionary, and Metrics (personCounts and errorCount)

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());
        final CSVReader reader = new CSVReader(Files.newBufferedReader(path));
        final Map<String, Integer> dataDictionary;

        try {
            dataDictionary = this.getDataDictionary(reader);
        } catch (final Throwable error) {
            reader.close();
            throw error;
        }

        // Process each person represented in the data set

        // One might be tempted to use Flux.fromIterable, but that would be a mistake:
        // * CSVReader is an Iterable that cannot be reused and Flux.fromIterable depends on this guarantee. It calls
        //   Iterable.spliterator twice starting out and this causes a read past the first record in our sample data.
        //   Consult the Flux.fromIterable code for specifics.
        // * Flux.fromStream guarantees that our reader will be closed when our operation is complete, regardless of
        //   how the operation finishes.

        final Optional<Integer> recordCount = Flux.fromStream(StreamSupport.stream(reader.spliterator(), false))
            .flatMap(
                line -> {

                    final Person person = new Person(
                        new PersonId(
                            line[dataDictionary.get("first_name")],
                            LocalDateTime.parse(line[dataDictionary.get("birth_date")]),
                            UUID.fromString(line[dataDictionary.get("uuid")])),
                        line[dataDictionary.get("last_name")],
                        line[dataDictionary.get("occupation")]);

                    return this.reactivePersonRepository.insert(person)
                        .doOnError(error -> System.out.println("failed to insert " + person + " due to " + error));

                })
            .parallel().runOn(Schedulers.parallel())
            .sequential().reduce(0, (subtotal, person) -> subtotal + 1)
            .blockOptional();

        System.out.println("Imported " + recordCount.orElse(0) + " person records from " + path);
    }

    @SuppressWarnings({ "LocalCanBeFinal", "unchecked" })
    private CompletableFuture<Map<Person, ?>[]> reactivelyTabulateYoungerPeopleThanEachPerson(final int iteration)
        throws IOException, URISyntaxException {

        // Setup our CSV Reader, Data dictionary, and Metrics (personCounts and errorCount)

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());
        final CSVReader reader = new CSVReader(Files.newBufferedReader(path));
        final Map<String, Integer> dataDictionary;

        try {
            dataDictionary = this.getDataDictionary(reader);
        } catch (Throwable error) {
            reader.close();
            throw error;
        }

        final ConcurrentMap<Person, Metrics> requestMetrics = new ConcurrentHashMap<>();
        final ConcurrentMap<Person, List<Person>> results = new ConcurrentHashMap<>();
        final CompletableFuture<Map<Person, ?>[]> future = new CompletableFuture<>();

        // Process each person represented in the data set

        // One might be tempted to use Flux.fromIterable, but that would be a mistake:
        // * CSVReader is an Iterable that cannot be reused and Flux.fromIterable depends on this guarantee. It calls
        //   Iterable.spliterator twice starting out and this causes a read past the first record in our sample data.
        //   Consult the Flux.fromIterable code for specifics.
        // * Flux.fromStream guarantees that our reader will be closed when our operation is complete, regardless of
        //   how the operation finishes.

        Flux.fromStream(StreamSupport.stream(reader.spliterator(), false)).flatMap(line -> {

            final Person elder = new Person(
                new PersonId(
                    line[dataDictionary.get("first_name")],
                    LocalDateTime.parse(line[dataDictionary.get("birth_date")]),
                    UUID.fromString(line[dataDictionary.get("uuid")])),
                line[dataDictionary.get("last_name")],
                line[dataDictionary.get("occupation")]);

            final LocalDateTime date = elder.getId().getBirthDate();
            final Flux<Person> youngerPeople = this.reactivePersonRepository.findByIdBirthDateGreaterThan(date);

            requestMetrics.compute(elder, (person, metrics) -> {
                if (metrics == null) {
                    metrics = new Metrics();
                }
                metrics.incrementRequests();
                return metrics;
            });

            return youngerPeople.collectSortedList()
                .map(sortedList -> new Object[] { elder, sortedList })
                .doOnError(error -> requestMetrics.get(elder).addError(error));

        }).parallel().runOn(Schedulers.parallel()).subscribe(
            result ->
                results.compute((Person) result[0], (elder, youngerPeople) -> (List<Person>) result[1]),
            error ->
                System.out.println("Iteration " + iteration + " error: '" + error + "'"),
            () -> {
                Map<Person, ?>[] value = (Map<Person, ?>[]) Array.newInstance(Map.class, 2);
                value[0] = results;
                value[1] = requestMetrics;
                future.complete(value);
            }
        );

        return future;
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void tabulatePeopleWithSameFirstName() throws CsvValidationException, IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {

            final Map<String, Integer> fields = this.getDataDictionary(reader);
            final Set<String> firstNames = new HashSet<>();

            String[] line;

            while ((line = reader.readNext()) != null) {

                final String firstName = line[fields.get("first_name")];

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

        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {

            final Map<String, Integer> fields = this.getDataDictionary(reader);
            final Set<String> lastNames = new HashSet<>();

            String[] line;

            while ((line = reader.readNext()) != null) {

                final String lastName = line[fields.get("last_name")];

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
    private void tabulatePeopleWithSameOccupation() throws CsvValidationException, IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {

            final Map<String, Integer> fields = this.getDataDictionary(reader);
            final Set<String> occupations = new HashSet<>();

            String[] line;

            while ((line = reader.readNext()) != null) {

                final String occupation = line[fields.get("occupation")];

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
    private void tabulateYoungerPeopleThanEachPerson() throws CsvValidationException, IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {

            final Map<String, Integer> fieldNames = this.getDataDictionary(reader);
            String[] line;

            while ((line = reader.readNext()) != null) {

                final Person person = new Person(
                    new PersonId(
                        line[fieldNames.get("first_name")],
                        LocalDateTime.parse(line[fieldNames.get("birth_date")]),
                        UUID.fromString(line[fieldNames.get("uuid")])),
                    line[fieldNames.get("last_name")],
                    line[fieldNames.get("occupation")]);

                final LocalDateTime dateTime = person.getId().getBirthDate();
                final List<Person> youngerPeople = this.personRepository.findByIdBirthDateGreaterThan(dateTime);

                System.out.println("People younger than: " + person);

                for (final Person youngerPerson : youngerPeople) {
                    System.out.println("  " + youngerPerson);
                }
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
