import shutil

from typing import Literal

from git import Repo
from prefect import flow, get_run_logger
from prefect.runtime.flow_run import get_run_count
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

DBT_PROJECT_DIR = "dbt_project"
DBT_REPO_URL_TEMPLATE = "https://github.com/dbt-labs/{}.git"  # Replace this template with your own org's git url, leaving the {} as the placeholder for the dbt repo name

Repository = Literal["jaffle_shop_duckdb"]

@flow(
    description=(
        "Runs commands `dbt deps` then `dbt build` by default. "
        "Runs `dbt retry` if the flow is retrying."
    ),
    retries=2,
    flow_run_name="dbt run: {repository}",
)
def dbt_flow(
    repository: Repository = "jaffle_shop_duckdb", commands: list[str] | None = None
):  # Replace the default dbt repo with your own
    try:
        if commands is None:
            commands = ["deps", "build"]

        repo_url = DBT_REPO_URL_TEMPLATE.format(repository)

        get_run_logger().info(f"Cloning repo {repo_url} to {DBT_PROJECT_DIR}")
        Repo.clone_from(repo_url, DBT_PROJECT_DIR)

        runner = PrefectDbtRunner(
            settings=PrefectDbtSettings(project_dir=DBT_PROJECT_DIR),
            include_compiled_code=True,
        )

        if get_run_count() == 1:
            for command in commands:
                runner.invoke(command.split(" "))
        else:
            runner.invoke(["retry"])

    finally:
        shutil.rmtree(DBT_PROJECT_DIR)


if __name__ == "__main__":
    dbt_flow(commands=["build --target snowflake"])