// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra;

import com.azure.cosmos.cassandra.data.Person;
import com.azure.cosmos.cassandra.data.PersonId;
import com.azure.cosmos.cassandra.data.PersonRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Runs the application with output written the standard output device.
 */
@SpringBootApplication
public class CosmosCassandraCommandLineRunner implements CommandLineRunner {

    private final PersonRepository personRepository;

    /**
     * Initializes a new instance of the {@link CosmosCassandraCommandLineRunner application}.
     *
     * @param personRepository a reference to a repository instance containing people.
     */
    public CosmosCassandraCommandLineRunner(final PersonRepository personRepository) {
        this.personRepository = personRepository;
    }

    /**
     * The main {@linkplain CosmosCassandraCommandLineRunner application} entry point.
     *
     * @param args an array of arguments.
     */
    public static void main(final String[] args) {
        SpringApplication.run(CosmosCassandraCommandLineRunner.class);
    }

    /**
     * Runs the {@linkplain CosmosCassandraCommandLineRunner application} logic.
     *
     * This method is called by Spring Boot after it instantiates the {@linkplain CosmosCassandraCommandLineRunner
     * application}.
     *
     * @param args a variable argument list.
     *
     * @throws Exception if an error occurs.
     */
    @Override
    public void run(final String... args) throws Exception {

        final PersonId key = new PersonId("John", LocalDateTime.now(), UUID.randomUUID());
        final Person p = new Person(key, "Doe", "Software Developer");
        this.personRepository.insert(p);

        System.out.println("find by first name");
        this.personRepository.findByIdFirstName("John").forEach(System.out::println);

        System.out.println("find by first name and date of birth greater than date");
        this.personRepository
            .findByIdFirstNameAndIdDateOfBirthGreaterThan("John", LocalDateTime.now().minusDays(1))
            .forEach(System.out::println);

        System.out.println("find by last name");
        this.personRepository.findByLastName("Doe").forEach(System.out::println);
    }
}
