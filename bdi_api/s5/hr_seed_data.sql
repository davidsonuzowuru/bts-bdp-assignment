INSERT INTO department (id, name, location) VALUES
(1, 'Engineering', 'New York'),
(2, 'Marketing', 'Los Angeles'),
(3, 'Finance', 'Chicago');

INSERT INTO employee (id, first_name, last_name, email, salary, hire_date, department_id) VALUES
(1, 'Alice', 'Smith', 'alice@example.com', 95000.00, '2020-01-15', 1),
(2, 'Bob', 'Johnson', 'bob@example.com', 85000.00, '2019-03-22', 1),
(3, 'Carol', 'Williams', 'carol@example.com', 72000.00, '2021-07-01', 2),
(4, 'David', 'Brown', 'david@example.com', 68000.00, '2020-11-10', 2),
(5, 'Eve', 'Davis', 'eve@example.com', 90000.00, '2018-05-30', 3);

INSERT INTO project (id, name, department_id) VALUES
(1, 'Project Alpha', 1),
(2, 'Project Beta', 1),
(3, 'Project Gamma', 2);

INSERT INTO employee_project (employee_id, project_id) VALUES
(1, 1),
(1, 2),
(2, 1),
(3, 3),
(4, 3);

INSERT INTO salary_history (id, employee_id, change_date, old_salary, new_salary, reason) VALUES
(1, 1, '2021-01-01', 85000.00, 95000.00, 'Annual raise'),
(2, 1, '2022-01-01', 95000.00, 100000.00, 'Promotion'),
(3, 2, '2021-06-01', 78000.00, 85000.00, 'Performance bonus'),
(4, 5, '2020-01-01', 82000.00, 90000.00, 'Annual raise');
