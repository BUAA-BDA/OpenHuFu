drop table if exists student;
create table student (
    name varchar,
    age int,
    score int,
    dept_name varchar
);

insert into student values('john', 22, 100, 'computer'),('jack', 23, 60, 'physics'),('Brand', 20, 80, 'electronics');