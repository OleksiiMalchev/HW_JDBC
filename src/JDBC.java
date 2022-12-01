import domain.Author;
import domain.Book;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBC {
    static final Connection connection;
    static final String URL = "jdbc:mysql://localhost:3306/book_store";
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //Вибрати з бази всі книги, та авторів, які їх написали.
    // Для книг без автора поле залишиться null.
    // Результат повинен містити поля BookTitle, AuthorName, AuthorLastName (використовуйте псевдоніми).
    public static List<Book> getAllBooksWithAuthors() throws SQLException {
        List<Book> booksWithAuthor = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            String SQL = "SELECT b.id, Title AS BookTitle, Name AS AuthorName, LastName AS AuthorLastName\n" +
                    "FROM books AS b\n" +
                    "LEFT JOIN authors AS a \n" +
                    "ON b.AuthorId = a.id";
            ResultSet resultSet = statement.executeQuery(SQL);
            while (resultSet.next()) {
                final Long id = resultSet.getLong("id");
                final String title = resultSet.getString("BookTitle");
                final String authorName = resultSet.getString("AuthorName");
                final String authorLastName = resultSet.getString("AuthorLastName");
                Author author = new Author(authorName, authorLastName);
                Book book = new Book(id, title);
                book.setAuthor(author);
                booksWithAuthor.add(book);
            }
            return booksWithAuthor;
        }
    }

    //Вибрати всі книги без автора. Результат повинен містити BookTitle.
    public static List<Book> getAllBooksWithOutAuthor() throws SQLException {
        List<Book> booksWithOutAuthor = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            String SQL = "SELECT id, Title AS BookTitle, AuthorId  \n" +
                    "FROM books \n" +
                    "WHERE AuthorId IS NULL";
            ResultSet resultSet = statement.executeQuery(SQL);
            while (resultSet.next()) {
                final Long id = resultSet.getLong("id");
                final String title = resultSet.getString("BookTitle");
                Book book = new Book(id, title);
                booksWithOutAuthor.add(book);
            }
            return booksWithOutAuthor;
        }
    }

    //Вибрати з бази всіх авторів, та кількість книг які вони написали. Результат повинен містити поля AuthorName,
    // AuthorLastName, BookCount.
    public static void getAllAuthorsAndCountWrittenBooks() throws SQLException {
        System.out.println("method getAllAuthorsAndCountWrittenBooks()");
        try (Statement statement = connection.createStatement()) {
            String SQL = "SELECT a.id, Name AS AuthorName, LastName AS AuthorLastName,count(b.id) AS BookCount\n" +
                    "FROM books AS b\n" +
                    "INNER JOIN authors AS a ON b.AuthorId = a.id\n" +
                    "GROUP BY a.id";
            ResultSet resultSet = statement.executeQuery(SQL);
            while (resultSet.next()) {
                final Long id = resultSet.getLong("id");
                final String authorName = resultSet.getString("AuthorName");
                final String authorLastName = resultSet.getString("AuthorLastName");
                final int count = resultSet.getInt("BookCount");
                Author author = new Author(id, authorName, authorLastName);
                System.out.println(author + " " + " " + "wrote " + count + " books");
            }
        }
    }

    //Вибрати з бази всіх авторів, які написали більше двох книг. Результат повинен містити поля AuthorName,
    // AuthorLastName, BookCount.
    public static void getAllAuthorWhoWroteMoreThanTwoBooks() throws SQLException {

        System.out.println("method getAllAuthorWhoWroteMoreThanTwoBooks()");
        try (Statement statement = connection.createStatement()) {
            String SQL = "SELECT a.id, Name AS AuthorName, LastName AS AuthorLastName,count(b.id) AS BookCount\n" +
                    "FROM books AS b\n" +
                    "INNER JOIN authors AS a ON b.AuthorId = a.id\n" +
                    "GROUP BY a.id\n" +
                    "HAVING count(b.id)>2;";
            ResultSet resultSet = statement.executeQuery(SQL);
            while (resultSet.next()) {
                final Long id = resultSet.getLong("id");
                final String authorName = resultSet.getString("AuthorName");
                final String authorLastName = resultSet.getString("AuthorLastName");
                final int count = resultSet.getInt("BookCount");
                Author author = new Author(id, authorName, authorLastName);
                System.out.println(author + " " + " " + "wrote " + count + " books");
            }
        }
    }


    public static void main(String[] args) throws SQLException {
        List<Book> allBookWithAuthor = getAllBooksWithAuthors();
        System.out.println("method getAllBooksWithAuthors()" + allBookWithAuthor);
        List<Book> allBooksWithOutAuthor = getAllBooksWithOutAuthor();
        System.out.println("method getAllBooksWithOutAuthor()" + allBooksWithOutAuthor);
        getAllAuthorsAndCountWrittenBooks();
        getAllAuthorWhoWroteMoreThanTwoBooks();
    }
}