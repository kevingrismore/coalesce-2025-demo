import shutil
import json
import os

from typing import Literal

from git import Repo
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.runtime.flow_run import get_run_count
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

DBT_PROJECT_DIR = "dbt_project"

Repository = Literal["https://github.com/dbt-labs/jaffle_shop_duckdb.git", "https://github.com/fivetran/dbt_github.git", "https://github.com/kevingrismore/coalesce-2025-github-fivetran.git"]

@flow(
    description=(
        "Runs commands `dbt deps` then `dbt build` by default. "
        "Runs `dbt retry` if the flow is retrying."
    ),
    retries=2,
    flow_run_name="dbt run: {repo_url}",
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
            raise_on_failure=False,
        )

        if get_run_count() == 1:
            for command in commands:
                runner.invoke(command.split(" "))
        else:
            runner.invoke(["retry"])

        create_markdown_report(DBT_PROJECT_DIR)

    finally:
        shutil.rmtree(DBT_PROJECT_DIR)

@task
def create_markdown_report(project_dir: str):
    """Generate a comprehensive markdown report from dbt run_results.json"""
    run_results_path = os.path.join(project_dir, "target", "run_results.json")
    
    if not os.path.exists(run_results_path):
        logger = get_run_logger()
        logger.warning(f"run_results.json not found at {run_results_path}")
        return
    
    with open(run_results_path, "r") as f:
        run_results = json.load(f)
    
    markdown_report = generate_dbt_markdown_report(run_results)
    
    create_markdown_artifact(
        markdown=markdown_report,
        key="dbt-report",
    )

def generate_dbt_markdown_report(run_results: dict) -> str:
    """
    Generate a comprehensive markdown report from dbt run_results.json data.
    
    Args:
        run_results: Dictionary containing dbt run results data
        
    Returns:
        Formatted markdown report string
    """
    from datetime import datetime
    
    # Extract metadata
    metadata = run_results.get('metadata', {})
    dbt_version = metadata.get('dbt_version', 'Unknown')
    generated_at = metadata.get('generated_at', 'Unknown')
    
    # Format timestamp
    if generated_at != 'Unknown':
        try:
            # Handle both ISO format with Z and without
            if generated_at.endswith('Z'):
                generated_at = generated_at[:-1] + '+00:00'
            dt = datetime.fromisoformat(generated_at)
            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except:
            formatted_time = generated_at
    else:
        formatted_time = 'Unknown'
    
    # Initialize report sections
    report_lines = [
        "# 🚀 dbt Run Report",
        "",
        f"**Generated at:** {formatted_time}",
        f"**dbt Version:** {dbt_version}",
        "",
        "---",
        "",
        "## 📊 Executive Summary",
        ""
    ]
    
    # Process results and calculate metrics
    results = run_results.get('results', [])
    total_nodes = len(results)
    
    # Initialize counters
    status_counts = {}
    node_types = {}
    execution_times = []
    errors = []
    warnings = []
    
    # Process each result
    for result in results:
        status = result.get('status', 'unknown').lower()
        status_counts[status] = status_counts.get(status, 0) + 1
        
        # Track node types
        node_type = result.get('node_type', 'unknown')
        node_types[node_type] = node_types.get(node_type, 0) + 1
        
        # Track execution times
        exec_time = result.get('execution_time', 0)
        if exec_time > 0:
            execution_times.append(exec_time)
        
        # Track errors and warnings
        if status == 'error':
            errors.append({
                'unique_id': result.get('unique_id', 'Unknown'),
                'message': result.get('message', 'No error message'),
                'execution_time': exec_time
            })
        elif status == 'warn':
            warnings.append({
                'unique_id': result.get('unique_id', 'Unknown'),
                'message': result.get('message', 'No warning message'),
                'execution_time': exec_time
            })
    
    # Calculate performance metrics
    total_execution_time = sum(execution_times)
    avg_execution_time = total_execution_time / len(execution_times) if execution_times else 0
    max_execution_time = max(execution_times) if execution_times else 0
    min_execution_time = min(execution_times) if execution_times else 0
    
    # Success rate calculation
    success_count = status_counts.get('success', 0)
    success_rate = (success_count / total_nodes * 100) if total_nodes > 0 else 0
    
    # Add summary metrics
    report_lines.extend([
        f"- **Total Nodes Executed:** {total_nodes}",
        f"- **Success Rate:** {success_rate:.1f}%",
        f"- **Total Execution Time:** {total_execution_time:.2f} seconds",
        f"- **Average Execution Time:** {avg_execution_time:.2f} seconds",
        "",
        "### Status Breakdown",
        ""
    ])
    
    # Add status breakdown
    for status, count in sorted(status_counts.items()):
        percentage = (count / total_nodes * 100) if total_nodes > 0 else 0
        status_emoji = {
            'success': '✅',
            'error': '❌',
            'skipped': '⏭️',
            'warn': '⚠️',
            'pass': '✅',
            'fail': '❌'
        }.get(status, '❓')
        
        report_lines.append(f"- {status_emoji} **{status.title()}:** {count} ({percentage:.1f}%)")
    
    report_lines.extend(["", "### Node Types", ""])
    
    # Add node type breakdown
    for node_type, count in sorted(node_types.items()):
        percentage = (count / total_nodes * 100) if total_nodes > 0 else 0
        report_lines.append(f"- **{node_type.title()}:** {count} ({percentage:.1f}%)")
    
    # Performance analysis
    if execution_times:
        report_lines.extend([
            "",
            "## ⚡ Performance Analysis",
            "",
            f"- **Fastest Node:** {min_execution_time:.2f} seconds",
            f"- **Slowest Node:** {max_execution_time:.2f} seconds",
            f"- **Performance Range:** {max_execution_time - min_execution_time:.2f} seconds",
            ""
        ])
        
        # Identify slow nodes (top 5)
        slow_nodes = sorted(results, key=lambda x: x.get('execution_time', 0), reverse=True)[:5]
        if slow_nodes and slow_nodes[0].get('execution_time', 0) > 0:
            report_lines.extend([
                "### 🐌 Slowest Nodes",
                ""
            ])
            for i, node in enumerate(slow_nodes, 1):
                exec_time = node.get('execution_time', 0)
                if exec_time > 0:
                    report_lines.append(f"{i}. **{node.get('unique_id', 'Unknown')}** - {exec_time:.2f}s")
            report_lines.append("")
    
    # Error analysis
    if errors:
        report_lines.extend([
            "## ❌ Error Analysis",
            ""
        ])
        for error in errors:
            report_lines.extend([
                f"### {error['unique_id']}",
                f"- **Error:** {error['message']}",
                f"- **Execution Time:** {error['execution_time']:.2f} seconds",
                ""
            ])
    
    # Warning analysis
    if warnings:
        report_lines.extend([
            "## ⚠️ Warning Analysis",
            ""
        ])
        for warning in warnings:
            report_lines.extend([
                f"### {warning['unique_id']}",
                f"- **Warning:** {warning['message']}",
                f"- **Execution Time:** {warning['execution_time']:.2f} seconds",
                ""
            ])
    
    # Detailed results section
    report_lines.extend([
        "## 📋 Detailed Results",
        ""
    ])
    
    # Group results by status for better organization
    status_groups = {}
    for result in results:
        status = result.get('status', 'unknown')
        if status not in status_groups:
            status_groups[status] = []
        status_groups[status].append(result)
    
    # Add detailed results by status
    for status in ['success', 'error', 'warn', 'skipped', 'pass', 'fail']:
        if status in status_groups:
            status_emoji = {
                'success': '✅',
                'error': '❌',
                'skipped': '⏭️',
                'warn': '⚠️',
                'pass': '✅',
                'fail': '❌'
            }.get(status, '❓')
            
            report_lines.extend([
                f"### {status_emoji} {status.title()} ({len(status_groups[status])})",
                ""
            ])
            
            for result in status_groups[status]:
                unique_id = result.get('unique_id', 'Unknown')
                execution_time = result.get('execution_time', 0)
                message = result.get('message', 'No message')
                node_type = result.get('node_type', 'unknown')
                
                report_lines.extend([
                    f"**{unique_id}** ({node_type})",
                    f"- Execution Time: {execution_time:.2f}s",
                    f"- Message: {message}",
                    ""
                ])
    
    # Add footer
    report_lines.extend([
        "---",
        "",
        f"*Report generated from dbt run_results.json*",
        f"*Total processing time: {total_execution_time:.2f} seconds*"
    ])
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    dbt_flow(commands=["build --target snowflake"])