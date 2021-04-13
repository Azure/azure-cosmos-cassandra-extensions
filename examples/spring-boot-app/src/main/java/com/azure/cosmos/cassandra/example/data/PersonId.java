// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example.data;

import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

import static org.springframework.data.cassandra.core.cql.Ordering.DESCENDING;
import static org.springframework.data.cassandra.core.cql.PrimaryKeyType.PARTITIONED;

/**
 * Represents the identity of a {@link Person} entity. In this example, a person is identified by first name, date of
 * birth, and a UUID. This enables fast lookups of a person by first name and data of birth, or UUID.
 */
@PrimaryKeyClass
public class PersonId implements Serializable {

    // region Fields

    private static final long serialVersionUID = 5609379476555574274L;

    @PrimaryKeyColumn(name = "date_of_birth", ordinal = 0)
    private final LocalDateTime dateOfBirth;

    @PrimaryKeyColumn(name = "first_name", type = PARTITIONED)
    private final String firstName;

    @PrimaryKeyColumn(name = "uuid", ordinal = 1, ordering = DESCENDING)
    private final UUID uuid;

    // endregion

    // region Constructors

    /**
     * Initializes a new {@link PersonId}.
     *
     * @param firstName first name of the {@linkplain Person person}.
     * @param dateOfBirth date of birth of the {@linkplain Person person}.
     * @param uuid ID of the {@linkplain Person person}.
     */
    public PersonId(final String firstName, final LocalDateTime dateOfBirth, final UUID uuid) {
        this.dateOfBirth = dateOfBirth;
        this.firstName = firstName;
        this.uuid = uuid;
    }

    // endregion

    // region Accessors

    /**
     * Returns the date of birth of the {@link Person} identified by this {@link PersonId}.
     *
     * @return the date of birth of the {@link Person} identified by this {@link PersonId}.
     */
    public LocalDateTime getDateOfBirth() {
        return this.dateOfBirth;
    }

    /**
     * Returns the first name of the {@link Person} identified by this {@link PersonId}.
     *
     * @return the first name of the {@link Person} identified by this {@link PersonId}.
     */
    public String getFirstName() {
        return this.firstName;
    }

    /**
     * Returns the ID of the {@link Person} identified by this {@link PersonId}.
     *
     * @return the ID of the {@link Person} identified by this {@link PersonId}.
     */
    public UUID getUuid() {
        return this.uuid;
    }

    // endregion

    // region Methods

    @Override
    public boolean equals(final Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || this.getClass() != other.getClass()) {
            return false;
        }

        final PersonId personId = (PersonId) other;

        return this.dateOfBirth.equals(personId.dateOfBirth)
            && this.firstName.equals(personId.firstName)
            && this.uuid.equals(personId.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.dateOfBirth, this.firstName, this.uuid);
    }

    @Override
    public String toString() {
        return "{"
            + "firstName='"
            + this.firstName
            + '\''
            + ",dateOfBirth="
            + this.dateOfBirth
            + ",uuid="
            + this.uuid
            + '}';
    }

    // endregion
}
