// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.data;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Represents a database with methods for looking up people.
 */
@Repository
public interface PersonRepository extends CassandraRepository<Person, PersonId> {

    /**
     * Finds all {@linkplain Person people} with the same first name.
     *
     * @param firstName The first name to find.
     *
     * @return The list of {@linkplain Person people} found.
     */
    List<Person> findByIdFirstName(String firstName);

    /**
     * Finds all {@linkplain Person people} with the same first name and a birth date after a specified local date time.
     *
     * @param firstName The first name to find.
     * @param dateTime  A local date time.
     *
     * @return The list of {@linkplain Person people} found.
     */
    List<Person> findByIdFirstNameAndIdDateOfBirthGreaterThan(String firstName, LocalDateTime dateTime);

    /**
     * Finds all {@linkplain Person people} with the same last name.
     *
     * @param lastName The last name to find.
     *
     * @return The list of {@linkplain Person people} found.
     */
    @Query(allowFiltering = true)
    List<Person> findByLastName(String lastName);
}
