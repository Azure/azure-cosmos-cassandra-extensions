// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example.data;

import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;


/**
 * Represents a database with methods for looking up people.
 */
@Repository
public interface ReactivePersonRepository extends ReactiveCassandraRepository<Person, PersonId> {

    /**
     * Finds all {@linkplain Person people} with the same first name.
     *
     * @param firstName The first name to find.
     *
     * @return A list of {@linkplain Person people} with the same first name.
     */
    Flux<Person> findByIdFirstName(String firstName);

    /**
     * Finds all {@linkplain Person people} born after the given local {@code date} time.
     *
     * @param dateTime A {@linkplain LocalDateTime local date time}.
     *
     * @return The list of {@linkplain Person people} born after the given {@code date}.
     */
    @Query(allowFiltering = true)
    Flux<Person> findByIdBirthDateGreaterThan(LocalDateTime dateTime);

    /**
     * Finds all {@linkplain Person people} with the same first name and a birth date after the given local date time.
     *
     * @param firstName The first name to find.
     * @param birthDate  A local date time.
     *
     * @return The list of {@linkplain Person people} found.
     */
    Flux<Person> findByIdFirstNameAndIdBirthDateGreaterThan(String firstName, LocalDateTime birthDate);

    /**
     * Finds all {@linkplain Person people} with the same last name.
     *
     * @param lastName The last name to find.
     *
     * @return The list of {@linkplain Person people} found.
     */
    @Query(allowFiltering = true)
    Flux<Person> findByLastName(String lastName);

    /**
     * Finds all {@linkplain Person people} with the same occupation.
     *
     * @param occupation The occupation to find.
     *
     * @return The list of {@linkplain Person people} found.
     */
    @Query(allowFiltering = true)
    Flux<Person> findByOccupation(String occupation);
}
