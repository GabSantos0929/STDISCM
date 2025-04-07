-- Create the database
CREATE DATABASE enrollment_system;

\c enrollment_system;

SELECT * FROM users;
SELECT * FROM courses;
SELECT * FROM sections;
SELECT * FROM section_schedule;
SELECT * FROM enrollments;
SELECT * FROM grades;

-- Create enumerated types
CREATE TYPE user_role AS ENUM ('student', 'faculty');
CREATE TYPE class_status AS ENUM ('open', 'closed');
CREATE TYPE class_modality AS ENUM ('F2F', 'HYBRID', 'FULLONLINE');
CREATE TYPE schedule_day AS ENUM ('M', 'T', 'W', 'H', 'F', 'S');
CREATE TYPE student_grade AS ENUM ('0.0', '1.0', '1.5', '2.0', '2.5', '3.0', '3.5', '4.0');

-- Create Users Table
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role user_role NOT NULL
);

-- Create Courses Table
CREATE TABLE courses (
    course_code VARCHAR(10) PRIMARY KEY,
    course_name VARCHAR(255) NOT NULL,
    units INTEGER NOT NULL
);

-- Create Sections Table
CREATE TABLE sections (
    section_id VARCHAR(5) PRIMARY KEY,
	class_number INTEGER UNIQUE NOT NULL,
    course_code VARCHAR(10) REFERENCES courses(course_code) ON DELETE CASCADE,
    status class_status NOT NULL,
	enrollment_cap INTEGER NOT NULL,
	enrolled INTEGER NOT NULL,
	remarks class_modality
);

-- Create Schedule Table
CREATE TABLE section_schedule (
    schedule_id SERIAL PRIMARY KEY,
    section_id VARCHAR(5) REFERENCES sections(section_id) ON DELETE CASCADE,
    day schedule_day NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
	room VARCHAR(10)
);

-- Create Enrollments Table
CREATE TABLE enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    section_id VARCHAR(5) REFERENCES sections(section_id) ON DELETE CASCADE
);

-- Create Grades Table
CREATE TABLE grades (
    grade_id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    section_id VARCHAR(5) REFERENCES sections(section_id) ON DELETE CASCADE,
    grade student_grade NOT NULL
);

-- Populate database here
INSERT INTO users (user_id, first_name, last_name, email, password, role) 
VALUES 
(12012345, 'Juan', 'Dela Cruz', 'juan@dlsu.edu.ph', 'juan123', 'student'),
(12112345, 'John', 'Doe', 'john@dlsu.edu.ph', 'john123', 'student'),
(10012345, 'Donald', 'Trump', 'trump@dlsu.edu.ph', 'trump123', 'faculty');

INSERT INTO courses (course_code, course_name, units) 
VALUES 
('STDISCM', 'DISTRIBUTED COMPUTING', 3),
('CSSECDV', 'SECURE WEB DEVELOPMENT', 3),
('CSOPESY', 'INTRODUCTION TO OPERATING SYSTEMS', 3);

INSERT INTO sections (section_id, class_number, course_code, status, enrollment_cap, enrolled, remarks) 
VALUES 
('S12', 4355, 'STDISCM', 'open', 45, 43, 'HYBRID'),
('S15', 6255, 'CSSECDV', 'closed', 30, 30, 'HYBRID'),
('S11', 1234, 'CSOPESY', 'open', 45, 0, 'HYBRID');

INSERT INTO section_schedule (section_id, day, start_time, end_time, room) 
VALUES 
('S12', 'T', '07:30:00', '09:00:00', NULL),
('S12', 'F', '09:15:00', '10:45:00', 'AG1110'),
('S15', 'T', '14:30:00', '16:00:00', NULL),
('S15', 'F', '14:30:00', '16:00:00', 'GK210'),
('S11', 'T', '16:15:00', '17:45:00', NULL),
('S11', 'F', '14:30:00', '16:00:00', 'GK201');

INSERT INTO enrollments (student_id, section_id) 
VALUES 
(12012345, 'S12'),
(12012345, 'S15'),
(12112345, 'S15'),
(12112345, 'S11');

INSERT INTO grades (student_id, section_id, grade)  
VALUES 
(12012345, 'S12', '3.0'),
(12012345, 'S15', '3.5'),
(12112345, 'S15', '2.0'),
(12112345, 'S11', '2.5');