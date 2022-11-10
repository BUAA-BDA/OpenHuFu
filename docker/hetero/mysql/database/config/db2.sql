drop table if exists student;
create table student (
    name varchar(50),
    age int,
    score int,
    dept_name varchar(50)
);

insert into student values('peter', 20, 71, 'computer'),('mary', 22, 82, 'math'),('Brown', 21, 88, 'software');