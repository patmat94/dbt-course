import json
from typing import Any, Mapping

from agate import default
from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets, default_metadata_from_dbt_resource_props

from .project import airbnb_project
from .constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path, exclude="fct_reviews")
def airbnb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

daily_partitions = DailyPartitionsDefinition(start_date="2022-01-24")

class CustomDagsterDbtTranlator(DagsterDbtTranslator):
    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = {"partition_expr": "date"}
        default_metadata = default_metadata_from_dbt_resource_props(dbt_resource_props)
        return {**default_metadata, **metadata}
    
@dbt_assets(manifest=dbt_manifest_path,
            select="fct_reviews",
            partitions_def=daily_partitions,
            dagster_dbt_translator=CustomDagsterDbtTranlator())

def dbtlearn_partitioned_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    partitioned_output_name = None
    for output_name in context.selected_output_names:
        try:
            context.asset_partitions_def_for_output(output_name)
            partitioned_output_name = output_name
            break
        except Exception:
            continue

    if partitioned_output_name is None:
        raise ValueError(
            "No partitioned output was found for dbtlearn_partitioned_dbt_assets. "
            "This run must target a partitioned model asset, not a dbt test asset."
        )

    first_partition, last_partition = context.asset_partitions_time_window_for_output(
        partitioned_output_name
    )
    dbt_vars = {"start_date": str(first_partition), "end_date": str(last_partition)}
    dbt_args = ["build", "--vars", json.dumps(dbt_vars)]
    dbt_cli_task = dbt.cli(dbt_args, context=context, raise_on_error=False)

    yield from dbt_cli_task.stream()

    