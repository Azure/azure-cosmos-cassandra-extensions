// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example;

import com.azure.cosmos.cassandra.example.data.Person;
import com.azure.cosmos.cassandra.example.data.PersonId;
import com.azure.cosmos.cassandra.example.data.PersonRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;
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
    @Override
    public void run(final String... args) {

        try {
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

        } catch (final Throwable error) {
            System.out.print("failed due to: ");
            error.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }
}
