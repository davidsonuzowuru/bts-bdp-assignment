CREATE TABLE department (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT NOT NULL
);

CREATE TABLE employee (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    salary NUMERIC(10, 2) NOT NULL,
    hire_date TEXT NOT NULL,
    department_id INTEGER REFERENCES department(id)
);

CREATE TABLE project (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER REFERENCES department(id)
);

CREATE TABLE employee_project (
    employee_id INTEGER REFERENCES employee(id),
    project_id INTEGER REFERENCES project(id),
    PRIMARY KEY (employee_id, project_id)
);

CREATE TABLE salary_history (
    id INTEGER PRIMARY KEY,
    employee_id INTEGER REFERENCES employee(id),
    change_date TEXT NOT NULL,
    old_salary NUMERIC(10, 2) NOT NULL,
    new_salary NUMERIC(10, 2) NOT NULL,
    reason TEXT
);
