import shutil

from typing import Literal

from git import Repo
from prefect import flow, get_run_logger
from prefect.runtime.flow_run import get_run_count
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

DBT_PROJECT_DIR = "dbt_project"

Repository = Literal["https://github.com/dbt-labs/jaffle_shop_duckdb.git", "https://github.com/fivetran/dbt_github.git"]

@flow(
    description=(
        "Runs commands `dbt deps` then `dbt build` by default. "
        "Runs `dbt retry` if the flow is retrying."
    ),
    retries=2,
    flow_run_name="dbt run: {repository}",
)
def dbt_flow(
    repo_url: Repository, commands: list[str] | None = None
):  # Replace the default dbt repo with your own
    try:
        if commands is None:
            commands = ["deps", "build"]

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