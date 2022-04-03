drop table if exists student;
create table student (
    name varchar,
    age int,
    score int,
    dept_name varchar
);

insert into student values('tom', 21, 90, 'computer'),('anna', 20, 89, 'software'),('Snow', 20, 99, 'software');