package org.example;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;

public class StudentEnrollmentApp {
    private static final String DB_NAME = "universityDB";   //  Get Database name
    private static final String STUDENTS_COLL = "students";   //  get collection
    private static final String COURSES_COLL = "courses";   //  get collection
    private static final String ENROLLMENTS_COLL = "enrollments";   //  get collection
    private static final String CONN_STRING = "mongodb://localhost:27017/";   //  connection strnig

    private final MongoClient mongoClient;
    private final MongoCollection<Document> students;
    private final MongoCollection<Document> courses;
    private final MongoCollection<Document> enrollments;

    public StudentEnrollmentApp() {
        mongoClient = MongoClients.create(CONN_STRING);   //  Connection String
        MongoDatabase database = mongoClient.getDatabase(DB_NAME);    //  Database Name

        students = database.getCollection(STUDENTS_COLL);   //  Get Collection
        courses = database.getCollection(COURSES_COLL);   //  Get Collection
        enrollments = database.getCollection(ENROLLMENTS_COLL);   //  Get Collection
    }

    public void initializeDatabase() {
        students.deleteMany(new Document());   //  Deletes all records from collection
        courses.deleteMany(new Document());   //  Deletes all records from collection
        enrollments.deleteMany(new Document());   //  Deletes all records from collection
        createIndexes();
    }

    private void createIndexes() {   //  to improve Data Retrieval, this stores a subset of the data in an ordered fashion.
        IndexOptions uniqueOption = new IndexOptions().unique(true);   //  ensures that certain indexes will enforce uniqueness, preventing duplicate values.
        students.createIndex(Indexes.ascending("studentId"), uniqueOption);   //  Creates an ascending index on the studentId field.
        students.createIndex(Indexes.text("name"));
        courses.createIndex(Indexes.ascending("courseCode"), uniqueOption);
    }

    public ObjectId addStudent(String studentId, String name) {
        Document student = new Document()
                .append("studentId", studentId)
                .append("name", name);
        students.insertOne(student);
        return student.getObjectId("_id");
    }

    public ObjectId addCourse(String courseCode, String courseName, int credits) {
        Document course = new Document()
                .append("courseCode", courseCode)
                .append("courseName", courseName)
                .append("credits", credits);
        courses.insertOne(course);
        return course.getObjectId("_id");
    }

    public void addEmbeddedEnrollment(ObjectId studentId, ObjectId courseId) {
        Document student = students.find(new Document("_id", studentId)).first();
        Document course = courses.find(new Document("_id", courseId)).first();

        Document enrollment = new Document()
                .append("type", "embedded")
                .append("student", student)
                .append("course", course)
                .append("enrollmentDate", new Date());
        enrollments.insertOne(enrollment);
    }

    public void addReferencedEnrollment(ObjectId studentId, ObjectId courseId) {
        Document enrollment = new Document()
                .append("type", "referenced")
                .append("studentId", studentId)
                .append("courseId", courseId)
                .append("enrollmentDate", new Date());
        enrollments.insertOne(enrollment);
    }

    public void printAllEnrollments() {
        System.out.println("ALL ENROLLMENTS:");
        for (Document enrollment : enrollments.find()) {
            if ("embedded".equals(enrollment.getString("type"))) {
                printEmbeddedEnrollment(enrollment);
            } else {
                printReferencedEnrollment(enrollment);
            }
            System.out.println("-----------------------");
        }
    }

    private void printEmbeddedEnrollment(Document enrollment) {
        Document student = enrollment.get("student", Document.class);
        Document course = enrollment.get("course", Document.class);

        System.out.println("EMBEDDED ENROLLMENT:");
        System.out.println("Student: " + student.getString("studentId") +
                " - " + student.getString("name"));
        System.out.println("Course: " + course.getString("courseCode") +
                " - " + course.getString("courseName") +
                " (" + course.getInteger("credits") + " credits)");
        System.out.println("Enrolled: " + enrollment.getDate("enrollmentDate"));
    }

    private void printReferencedEnrollment(Document enrollment) {
        Document student = students.find(
                new Document("_id", enrollment.getObjectId("studentId"))).first();
        Document course = courses.find(
                new Document("_id", enrollment.getObjectId("courseId"))).first();

        System.out.println("REFERENCED ENROLLMENT:");
        System.out.println("Student: " + student.getString("studentId") +
                " - " + student.getString("name"));
        System.out.println("Course: " + course.getString("courseCode") +
                " - " + course.getString("courseName") +
                " (" + course.getInteger("credits") + " credits)");
        System.out.println("Enrolled: " + enrollment.getDate("enrollmentDate"));
    }

    public void updateStudentName(ObjectId studentId, String newName) {
        UpdateResult result = students.updateOne(
                new Document("_id", studentId),
                new Document("$set", new Document("name", newName))
        );
        System.out.println("\nUpdated student name: " + result.getModifiedCount() + " document(s) modified");
    }

    public void close() {
        mongoClient.close();
    }

    public static void main(String[] args) {
        StudentEnrollmentApp app = new StudentEnrollmentApp();
        app.initializeDatabase();
        // Add sample students
        ObjectId student1Id = app.addStudent("S1001", "John Doe");
        ObjectId student2Id = app.addStudent("S1002", "Jane Smith");

        // Add sample courses
        ObjectId course1Id = app.addCourse("CS101", "Introduction to Programming", 3);
        ObjectId course2Id = app.addCourse("MATH201", "Calculus", 4);

        // Create enrollments
        app.addEmbeddedEnrollment(student1Id, course1Id);
        app.addReferencedEnrollment(student2Id, course2Id);

        // Display enrollments
        app.printAllEnrollments();

        // Update student name
        app.updateStudentName(student1Id, "Johnathan Doe");

        // Show effect of update
        System.out.println("\nAFTER STUDENT NAME UPDATE:");
        app.printAllEnrollments();

        app.close();
    }
}