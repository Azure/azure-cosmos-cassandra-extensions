// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.azure.cosmos.cassandra.example.data.Person;
import com.azure.cosmos.cassandra.example.data.PersonId;
import com.azure.cosmos.cassandra.example.data.PersonRepository;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Runs the application with output written the standard output device.
 */
@SpringBootApplication
public class ApplicationCommandLineRunner implements CommandLineRunner {

    private final PersonRepository personRepository;

    /**
     * Initializes a new instance of the {@link ApplicationCommandLineRunner application}.
     *
     * @param personRepository a reference to a repository instance containing people.
     */
    public ApplicationCommandLineRunner(@Autowired final PersonRepository personRepository) {
        this.personRepository = personRepository;
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
     * Runs the {@linkplain ApplicationCommandLineRunner application} logic.
     *
     * This method is called by Spring Boot after it instantiates the {@linkplain ApplicationCommandLineRunner
     * application}.
     *
     * @param args a variable argument list.
     */
    @SuppressFBWarnings("DM_EXIT")
    @Override
    public void run(final String... args) {

        try {
            this.importData();
            this.tabulatePeopleWithSameLastName();
            this.tabulatePeopleWithSameFirstName();
            this.tabulatePeopleWithSameOccupation();
            this.tabulateYoungerPeopleThanEachPerson();
        } catch (final Throwable error) {
            System.out.print("Application failed due to: ");
            error.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }

    private Map<String, Integer> getFields(final CSVReader reader) throws IOException, CsvValidationException {
        final Map<String, Integer> fieldNames = new HashMap<>();
        final String[] line = reader.readNext();

        for (int i = 0; i < line.length; i++) {
            fieldNames.put(line[i], i);
        }
        return fieldNames;
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void importData() throws IOException, URISyntaxException, CsvValidationException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {

            final Map<String, Integer> fieldNames = this.getFields(reader);
            String[] line;

            while ((line = reader.readNext()) != null) {

                final Person person = new Person(
                    new PersonId(
                        line[fieldNames.get("first_name")],
                        LocalDateTime.parse(line[fieldNames.get("birth_date")]),
                        UUID.fromString(line[fieldNames.get("uuid")])),
                    line[fieldNames.get("last_name")],
                    line[fieldNames.get("occupation")]);

                this.personRepository.insert(person);
            }
        }
    }

    @SuppressWarnings("LocalCanBeFinal")
    private void tabulatePeopleWithSameFirstName() throws CsvValidationException, IOException, URISyntaxException {

        final Path path = Paths.get(ClassLoader.getSystemResource("people.csv").toURI());

        try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {

            final Map<String, Integer> fields = this.getFields(reader);
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

            final Map<String, Integer> fields = this.getFields(reader);
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

            final Map<String, Integer> fields = this.getFields(reader);
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

            final Map<String, Integer> fieldNames = this.getFields(reader);
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
}
