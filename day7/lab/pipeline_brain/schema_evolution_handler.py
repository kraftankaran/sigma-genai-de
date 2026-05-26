from typing import Dict, List, Tuple, Union
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, FloatType, StructType, StructField

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]:
    """
    Detects schema drift between expected and actual schemas.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.

    Returns:
        Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]: A report on the schema drift.
    """
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: actual_schema[k] for k in expected_schema if expected_schema[k]!= actual_schema.get(k, '')}
    
    drift_severity = 'NONE'
    if new_columns:
        if any('null' not in v for v in new_columns.values()):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'

    return {
        'new_columns': new_columns,
       'removed_columns': list(removed_columns.keys()),
        'type_changes': type_changes,
        'drift_severity': drift_severity
    }

def decide_action(drift_report: Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]) -> Dict[str, Dict[str, Union[str, str, str]]]:
    """
    Decides the action to take for each column in the drift report.

    Args:
        drift_report (Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]): The drift report.

    Returns:
        Dict[str, Dict[str, Union[str, str, str]]]: A decision for each column.
    """
    decisions = {}
    for col_name, col_type in drift_report['new_columns'].items():
        if col_type.endswith('null'):
            decisions[col_name] = {'action': 'ADD_TO_SCHEMA','reason': 'Nullable new column', 'risk_level': 'LOW'}
        elif col_name == 'discount_amount':
            decisions[col_name] = {'action': 'FLAG_ANOMALY','reason': 'Potential revenue impact', 'risk_level': 'HIGH'}
        else:
            decisions[col_name] = {'action': 'ADD_TO_SCHEMA','reason': 'New string column', 'risk_level': 'LOW'}
    
    for col_name in drift_report['removed_columns']:
        decisions[col_name] = {'action': 'HALT','reason': 'Removed column', 'risk_level': 'BREAKING'}
    
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Union[str, str, str]]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    """
    Applies the schema evolution decisions to the DataFrame.

    Args:
        spark_df (DataFrame): The Spark DataFrame to evolve.
        decisions (Dict[str, Dict[str, Union[str, str, str]]]): The decisions for each column.
        updated_schema (Dict[str, str]): The updated schema.

    Returns:
        Tuple[DataFrame, List[str]]: The evolved DataFrame and a list of migration notes.
    """
    migration_notes = []
    for col_name, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(col_name)
        elif decision['action'] == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f"{col_name}_anomaly", F.when(F.col(col_name).isNull(), True).otherwise(False))
            migration_notes.append(f"Column '{col_name}' flagged for anomaly due to potential issues.")
        elif decision['action'] == 'ADD_TO_SCHEMA':
            if col_name not in spark_df.columns:
                spark_df = spark_df.withColumn(col_name, F.lit(None).cast(StringType() if col_name not in updated_schema else (FloatType() if col_name == 'discount_amount' else StringType())))
    
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Union[Dict, Dict, List, Dict]]:
    """
    Handles schema drift by detecting, deciding, and applying schema evolution.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.
        spark_df (DataFrame, optional): The Spark DataFrame to evolve. Defaults to None.

    Returns:
        Dict[str, Union[Dict, Dict, List, Dict]]: The full evolution report.
    """
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, {**expected_schema, **{k: v for k, v in actual_schema.items() if k not in expected_schema}})
        print(f"Schema drift report: {drift_report}")
        print(f"Schema evolution decisions: {decisions}")
        print(f"Migration notes: {migration_notes}")
        return {'drift_report': drift_report, 'decisions': decisions,'migration_notes': migration_notes, 'evolved_df': evolved_df}
    else:
        print(f"Schema drift report: {drift_report}")
        print(f"Schema evolution decisions: {decisions}")
        return {'drift_report': drift_report, 'decisions': decisions}
