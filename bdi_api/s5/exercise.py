from typing import Annotated
from pathlib import Path
from fastapi import APIRouter, status
from fastapi.params import Query
from sqlalchemy import create_engine, text
from bdi_api.settings import Settings

settings = Settings()

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)

SQL_DIR = Path(__file__).parent


def get_engine():
    return create_engine(settings.db_url)


def execute_sql_script(conn, sql: str):
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    for stmt in statements:
        conn.execute(text(stmt))


@s5.post("/db/init")
def init_database() -> str:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS salary_history"))
        conn.execute(text("DROP TABLE IF EXISTS employee_project"))
        conn.execute(text("DROP TABLE IF EXISTS project"))
        conn.execute(text("DROP TABLE IF EXISTS employee"))
        conn.execute(text("DROP TABLE IF EXISTS department"))
        schema_sql = (SQL_DIR / "hr_schema.sql").read_text(encoding="utf-8-sig")
        execute_sql_script(conn, schema_sql)
        conn.commit()
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    engine = get_engine()
    with engine.connect() as conn:
        seed_sql = (SQL_DIR / "hr_seed_data.sql").read_text(encoding="utf-8-sig")
        execute_sql_script(conn, seed_sql)
        conn.commit()
    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, name, location FROM department"))
        return [dict(row._mapping) for row in result]


@s5.get("/employees/")
def list_employees(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    per_page: Annotated[
        int,
        Query(description="Number of employees per page", ge=1, le=100),
    ] = 10,
) -> list[dict]:
    engine = get_engine()
    offset = (page - 1) * per_page
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT e.id, e.first_name, e.last_name, e.email, e.salary,
                   d.name AS department_name
            FROM employee e
            JOIN department d ON e.department_id = d.id
            LIMIT :limit OFFSET :offset
        """), {"limit": per_page, "offset": offset})
        return [dict(row._mapping) for row in result]


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, first_name, last_name, email, salary, hire_date
            FROM employee
            WHERE department_id = :dept_id
        """), {"dept_id": dept_id})
        return [dict(row._mapping) for row in result]


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                d.name AS department_name,
                COUNT(DISTINCT e.id) AS employee_count,
                AVG(e.salary) AS avg_salary,
                COUNT(DISTINCT ep.project_id) AS project_count
            FROM department d
            LEFT JOIN employee e ON e.department_id = d.id
            LEFT JOIN employee_project ep ON ep.employee_id = e.id
            WHERE d.id = :dept_id
            GROUP BY d.id, d.name
        """), {"dept_id": dept_id})
        row = result.fetchone()
        return dict(row._mapping) if row else {}


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT change_date, old_salary, new_salary, reason
            FROM salary_history
            WHERE employee_id = :emp_id
            ORDER BY change_date
        """), {"emp_id": emp_id})
        return [dict(row._mapping) for row in result]

