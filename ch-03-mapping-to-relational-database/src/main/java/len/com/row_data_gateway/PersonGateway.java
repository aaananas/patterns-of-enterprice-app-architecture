package len.com.row_data_gateway;


import lombok.Data;

import java.sql.*;
import java.util.Optional;

@Data
public class PersonGateway {
    private Integer id;
    private String lastName;
    private String firstName;
    private String email;

    public static Integer insert(String firstName, String lastName, String email) {
        try (Connection connection = getConnection();
             PreparedStatement statement =
                     connection.prepareStatement("INSERT INTO person (last_name, first_name, email) VALUES (?, ?, ?)",
                             Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, lastName);
            statement.setString(2, firstName);
            statement.setString(3, email);

            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            } else {
                throw new RuntimeException("No generated keys");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot insert person", e);
        }
    }


    public static Optional<PersonGateway> find(Integer id) {
        try (Connection connection = getConnection();
             PreparedStatement statement =
                     connection.prepareStatement("SELECT * FROM person WHERE id = ?")) {
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();
            return resultSet.next() ? Optional.of(parsePerson(resultSet)) : Optional.empty();
        } catch (SQLException e) {
            throw new RuntimeException("No person with id " + id, e);
        }
    }

    public Integer save() {
        try (Connection connection = getConnection();
             PreparedStatement statement =
                     connection.prepareStatement("INSERT INTO person (last_name, first_name, email) VALUES (?, ?, ?)",
                             Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, lastName);
            statement.setString(2, firstName);
            statement.setString(3, email);

            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            } else {
                throw new RuntimeException("No generated keys");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot create person", e);
        }
    }

    public void update() {
        try (Connection connection = getConnection();
             PreparedStatement statement =
                     connection.prepareStatement("UPDATE person SET last_name = ?, first_name = ?, email = ? WHERE id = ?")) {
            statement.setString(1, lastName);
            statement.setString(2, firstName);
            statement.setString(3, email);
            statement.setInt(4, id);

            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot update person with id " + id, e);
        }
    }

    private static PersonGateway parsePerson(ResultSet resultSet) throws SQLException {
        PersonGateway person = new PersonGateway();
        person.setId(resultSet.getInt("id"));
        person.setLastName(resultSet.getString("last_name"));
        person.setFirstName(resultSet.getString("first_name"));
        person.setEmail(resultSet.getString("email"));
        return person;
    }

    private static Connection getConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/db_name", "user", "pass");
        } catch (Exception e) {
            throw new RuntimeException("Couldn't connect to database", e);
        }
    }
}
