package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS Users (" +
            " id BIGINT AUTO_INCREMENT PRIMARY KEY" +
            ", name NVARCHAR(100)" +
            ", lastname NVARCHAR(100)" +
            ", age TINYINT);";
    private static final String DELETE_TABLE = "DROP TABLE IF EXISTS Users;";
    private static final String INSERT_USER = "INSERT INTO Users (name, lastname, age) VALUES ( ?, ?, ? );";
    private static final String DELETE_USER = "DELETE FROM Users WHERE id = ?;";
    private static final String SELECT_ALL_USERS = "SELECT name, lastname, age FROM Users ;";
    private static final String DELETE_ALL_USERS  = "DELETE FROM Users;";


    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {

        try (Connection conn = Util.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(CREATE_TABLE);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void dropUsersTable() {

        try (Connection conn = Util.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(DELETE_TABLE);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {

        try (Connection conn = Util.getConnection();
             PreparedStatement prepareStatement = conn.prepareStatement(INSERT_USER)) {
            prepareStatement.setString(1, name);
            prepareStatement.setString(2, lastName);
            prepareStatement.setByte(3, age);
            prepareStatement.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void removeUserById(long id) {

        try (Connection conn = Util.getConnection();
             PreparedStatement prepareStatement = conn.prepareStatement(DELETE_USER)) {
            prepareStatement.setLong(1, id);
            prepareStatement.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Connection conn = Util.getConnection();
             PreparedStatement prepareStatement = conn.prepareStatement(SELECT_ALL_USERS)) {

            ResultSet rs = prepareStatement.executeQuery();
            while (rs.next()) {
                String name = rs.getString("name");
                String lastname = rs.getString("lastname");
                byte age = rs.getByte("age");
                users.add(new User(name, lastname, age));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    public void cleanUsersTable() {


        try (Connection conn = Util.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(DELETE_ALL_USERS);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
