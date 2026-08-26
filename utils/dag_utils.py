# utils/dag_utils.py

def create_tracked_dag(
        dag_id,
        description=None,
        schedule=None,
        default_args=None,
        pipeline_name=None,
        client_id=None,
        data_type=None,
        **kwargs
):
    """
    Factory function to create a DAG with tracking capabilities.
    """
    from airflow import DAG

    # Initialize user_defined_macros if not provided
    if 'user_defined_macros' not in kwargs:
        kwargs['user_defined_macros'] = {}

    # Create the DAG
    dag = DAG(
        dag_id,
        description=description,
        schedule_interval=schedule,
        default_args=default_args,
        **kwargs
    )

    # Initialize user_defined_macros if not set
    if not hasattr(dag, 'user_defined_macros') or dag.user_defined_macros is None:
        dag.user_defined_macros = {}

    # Add tracking metadata to DAG
    dag.user_defined_macros.update({
        'pipeline_name': pipeline_name or dag_id,
        'client_id': client_id,
        'data_type': data_type,
    })

    return dag