// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.cassandra.example.data;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a {@linkplain Person person} identified by {@link PersonId} in a table of people.
 */
@Table("people")
public class Person implements Comparable<Person>, Serializable {

    // region Fields

    private static final long serialVersionUID = -1416817294690187846L;

    @PrimaryKey
    private final PersonId id;

    @Column("last_name")
    private final String lastName;

    @Column
    private final String occupation;

    // endregion

    // region Constructors

    /**
     * Initializes a new {@link Person} entity.
     *
     * @param id the {@linkplain Person person's} ID.
     * @param lastName the {@linkplain Person person's} last name.
     * @param occupation the {@linkplain Person person's} occupation.
     */
    public Person(final PersonId id, final String lastName, final String occupation) {
        this.id = id;
        this.lastName = lastName;
        this.occupation = occupation;
    }

    // endregion

    // region Accessors

    /**
     * Returns this {@linkplain Person person's} ID.
     *
     * @return this {@linkplain Person person's} ID.
     */
    public PersonId getId() {
        return this.id;
    }

    /**
     * Returns this {@linkplain Person person's} last name.
     *
     * @return this {@linkplain Person person's} last name.
     */
    public String getLastName() {
        return this.lastName;
    }

    /**
     * Returns this {@linkplain Person person's} occupation.
     *
     * @return this {@linkplain Person person's} occupation.
     */
    public String getOccupation() {
        return this.occupation;
    }

    // endregion

    // region Methods

    @Override
    public int compareTo(final Person other) {
        return this.id.compareTo(other.id);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || this.getClass() != other.getClass()) {
            return false;
        }
        final Person person = (Person) other;
        return this.id.equals(person.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    @Override
    public String toString() {
        return "{id:" + this.id + ",lastName:'" + this.lastName + "',occupation:'" + this.occupation + "'}";
    }

    // endregion
}
