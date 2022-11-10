drop table if exists student;
create table student (
    name varchar(50),
    age int,
    score int,
    dept_name varchar(50)
);

insert into student values('tom', 21, 90, 'computer'),('anna', 20, 89, 'software'),('Snow', 20, 99, 'software');