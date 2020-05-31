/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.spark.search.rdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;

class Person implements Serializable {
    public static final List<Person> PERSONS = Arrays.asList(
            new Person("André", null, 5, null, null, null),
            new Person(null, "Yulia", 2, null, null, null),
            new Person("Jorge", "Michael", 53),
            new Person("Bob", "Marley", 37),
            new Person("Agnès", "Bartoll", -1));

    private static final long serialVersionUID = 1L;

    private String firstName;
    private String lastName;
    private int age;
    private Date birthDate;
    private Address address;
    private List<Person> friends;

    Person(String firstName, String lastName, int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }

    Person(String firstName, String lastName, int age,
           Date birthDate,
           Address address,
           List<Person> friends) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.birthDate = birthDate;
        this.address = address;
        this.friends = friends;
    }

    public Person() {
        birthDate = null;
        address = null;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Address getAddress() {
        return address;
    }

    public List<Person> getFriends() {
        return friends;
    }

    public void setFriends(List<Person> friends) {
        this.friends = friends;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return age == person.age &&
                Objects.equals(firstName, person.firstName) &&
                Objects.equals(lastName, person.lastName) &&
                Objects.equals(birthDate, person.birthDate) &&
                Objects.equals(address, person.address) &&
                Objects.equals(friends, person.friends);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstName, lastName, age, birthDate, address, friends);
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                ", birthDate=" + birthDate +
                ", address=" + address +
                ", friends=" + friends +
                '}';
    }
}