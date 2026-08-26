import environmentconfig 
from datetime import datetime
from airflow.operators.python import PythonOperator
from utils.pipeline_tracker import PipelineTracker
from airflow.utils.email import send_email


class TrackedPythonOperator(PythonOperator):
    """
    A custom operator that automatically handles pipeline tracking.
    """

    def __init__(
            self,
            *args,
            pipeline_name=None,
            client_id=None,
            data_type=None,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=None,
            success_email_to=None,
            on_task_failure_callable=None,      
            **kwargs
    ):
        self.pipeline_name = pipeline_name
        self.client_id = client_id
        self.data_type = data_type
        self.is_first_task = is_first_task
        self.is_last_task = is_last_task
        self.failure_email_to = failure_email_to or []
        self.success_email_to = success_email_to or []
        self.on_task_failure_callable = on_task_failure_callable

        # Set callback BEFORE calling super().__init__()
        kwargs['on_failure_callback'] = self._handle_failure

        # NOW call parent __init__
        super().__init__(*args, **kwargs)

    def execute(self, context):
        # Initialize tracker
        tracker = PipelineTracker(db_prefix="audit_")
        # Get DAG run configuration
        dag_run = context.get('dag_run')
        dag = context.get('dag')

        # Ensure we have client_id and data_type
        client_id = self.client_id or 'default'
        data_type = self.data_type or 'unknown'
        pipeline_name = self.pipeline_name or dag.dag_id

        # Get the task parameters from op_kwargs
        function_kwargs = self.op_kwargs or {}

        # Store all task parameters for tracking
        tracking_parameters = {}

        # Add any important parameters to track
        if 'start_date' in function_kwargs:
            tracking_parameters['extraction_start_date'] = function_kwargs['start_date'].isoformat()
        if 'end_date' in function_kwargs:
            tracking_parameters['extraction_end_date'] = function_kwargs['end_date'].isoformat()

        # Add any other parameters from dag_run.conf
        if dag_run and dag_run.conf:
            tracking_parameters.update(dag_run.conf)

        # If first task, start tracking
        run_id = None
        if self.is_first_task:
            run_id = tracker.start_pipeline_run(
                pipeline_name=pipeline_name,
                client_id=client_id,
                data_type=data_type,
                parameters=tracking_parameters,
                created_by="airflow"
            )
            # Store run_id in XCom
            context['ti'].xcom_push(key='run_id', value=run_id)
        else:
            # Get run_id from XCom
            try:
                run_id = context['ti'].xcom_pull(key='run_id')
            except:
                # Fall back to task-instance specific ID if needed
                run_id = f"{dag.dag_id}_{context['ti'].task_id}_{context['ts_nodash']}"

        # Add run_id to kwargs for the function
        function_kwargs['run_id'] = run_id
        function_kwargs['tracker'] = tracker

        # Execute the Python callable
        self.op_kwargs = function_kwargs
        result = super().execute(context)

        # If last task, complete tracking
        if self.is_last_task and run_id:
            record_count = 0
            # Try to get record count from task result or XCom
            if isinstance(result, dict) and 'record_count' in result:
                record_count = result['record_count']

            tracker.complete_pipeline_run(
                run_id=run_id,
                status="success",
                record_count=record_count,
                error_details=""
            )
             # ← NEW: send success email from the last task
            self._send_success_email(context, record_count)

        return result
    
    def _handle_failure(self, context):
        """Called when task fails - updates tracker and sends email."""
        print("=" * 50)
        print("TASK FAILED - HANDLING FAILURE")
        print("=" * 50)

        # Get error info
        exception = context.get('exception', None)
        error_msg = str(exception) if exception else "Unknown error"

        # Update pipeline tracker (your existing logic)
        try:
            tracker = PipelineTracker(db_prefix="audit_")
            run_id = None
            try:
                run_id = context['ti'].xcom_pull(key='run_id')
            except:
                run_id = f"{context['dag'].dag_id}_{context['ti'].task_id}_{context['ts_nodash']}"

            if run_id:
                tracker.complete_pipeline_run(
                    run_id=run_id,
                    status="failed",
                    record_count=0,
                    error_details=error_msg
                )
        except Exception as e:
            print(f"Error updating tracker: {e}")

        # Send email
        self._send_failure_email(context, error_msg)

        # Run callable if provided
        if self.on_task_failure_callable:
            self.on_task_failure_callable()

    def _send_failure_email(self, context, error_msg):
        """Send email notification on task failure."""
        if not self.failure_email_to:
            return

        try:
            ti = context['task_instance']
            dag = context['dag']
            execution_date = context.get('execution_date', 'N/A')
            dag_run = context.get('dag_run')
            
            base_url = "http://10.5.57.202:8081"
            run_id = dag_run.run_id if dag_run else 'N/A'
            log_url = f"{base_url}/dags/{dag.dag_id}/grid?dag_run_id={run_id}&task_id={ti.task_id}&tab=logs"

            subject = f"[AIRFLOW TASK FAILED] {dag.dag_id}.{ti.task_id}"

            html_content = f"""
            <html>
            <body style="font-family: Arial, sans-serif; padding: 20px;">
                <h2 style="color: #d32f2f;">🚨 Task Failure Alert</h2>

                <table style="border-collapse: collapse; width: 100%; max-width: 600px; margin-top: 20px;">
                    <tr style="background: #ffebee;">
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">DAG</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{dag.dag_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Task</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{ti.task_id}</td>
                    </tr>
                    <tr style="background: #f5f5f5;">
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Pipeline</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{self.pipeline_name or 'N/A'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Client</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{self.client_id or 'N/A'}</td>
                    </tr>
                    <tr style="background: #f5f5f5;">
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Execution Date</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{execution_date}</td>
                    </tr>
                    <tr>
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Try Number</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{ti.try_number}</td>
                    </tr>
                </table>

                <h3 style="color: #d32f2f; margin-top: 20px;">Error Details:</h3>
                <pre style="background: #ffebee; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap;">{error_msg}</pre>

                <p style="margin-top: 20px;">
                    <a href="{log_url}" style="background: #1976d2; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">View Logs</a>
                </p>

                <hr style="margin-top: 30px; border: none; border-top: 1px solid #ddd;">
                <p style="color: #666; font-size: 12px;">
                    This is an automated alert from Apache Airflow.<br>
                    Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </body>
            </html>
            """

            print(f"Sending failure email to: {self.failure_email_to}")
            send_email(
                to=self.failure_email_to,
                subject=subject,
                html_content=html_content
            )
            print("Failure Email sent successfully!")

        except Exception as e:
            print(f"Failed to send failure email: {e}")

    def _send_success_email(self, context, record_count=0):
        """Send 1 email at the end with all task statuses — only if ALL passed."""
        if not self.success_email_to:
            return

        # ── Gate: skip if any task failed ──────────────────────
        try:
            dag_run = context.get('dag_run')
            all_tis = dag_run.get_task_instances()

            failed_tasks = [
                ti for ti in all_tis
                if ti.state in ('failed', 'upstream_failed')
                and ti.task_id != context['ti'].task_id
            ]

            if failed_tasks:
                failed_names = [ti.task_id for ti in failed_tasks]
                print(f"Skipping success email — these tasks failed: {failed_names}")
                return

        except Exception as e:
            print(f"Could not check task states, skipping success email: {e}")
            return

        # ── Build task rows for the table ──────────────────────
        try:
            dag_run = context.get('dag_run')
            all_tis = dag_run.get_task_instances()

            # exclude start/end empty operators, only show TrackedPythonOperator tasks
            tracked_tasks = [
                ti for ti in all_tis
                if ti.task_id not in ('start', 'end')
            ]

            task_rows = ""
            for idx, ti in enumerate(tracked_tasks):
                bg = "#f5f5f5" if idx % 2 == 0 else "#ffffff"
                task_rows += f"""
                    <tr style="background: {bg};">
                        <td style="padding: 12px; border: 1px solid #ddd;">{ti.task_id}</td>
                        <td style="padding: 12px; border: 1px solid #ddd; color: #2e7d32; font-weight: bold;">✅ {ti.state.upper()}</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{ti.duration or 'N/A'} sec</td>
                    </tr>
                """

        except Exception as e:
            print(f"Could not build task rows: {e}")
            task_rows = "<tr><td colspan='3'>Could not load task details</td></tr>"

        try:
            ti = context['task_instance']
            dag = context['dag']
            execution_date = context.get('execution_date', 'N/A')
            dag_run = context.get('dag_run')
            base_url = "http://10.5.57.202:8081"
            run_id = dag_run.run_id if dag_run else 'N/A'
            log_url = f"{base_url}/dags/{dag.dag_id}/grid?dag_run_id={run_id}&tab=details"

            subject = f"[AIRFLOW PIPELINE SUCCESS] {dag.dag_id}"

            html_content = f"""
            <html>
            <body style="font-family: Arial, sans-serif; padding: 20px;">
                <h2 style="color: #2e7d32;">✅ Pipeline Success</h2>

                <table style="border-collapse: collapse; width: 100%; max-width: 600px; margin-top: 20px;">
                    <tr style="background: #e8f5e9;">
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">DAG</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{dag.dag_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Pipeline</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{self.pipeline_name or 'N/A'}</td>
                    </tr>
                    <tr style="background: #f5f5f5;">
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Client</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{self.client_id or 'N/A'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 12px; border: 1px solid #ddd; font-weight: bold;">Execution Date</td>
                        <td style="padding: 12px; border: 1px solid #ddd;">{execution_date}</td>
                    </tr>
                </table>

                <h3 style="margin-top: 25px; color: #2e7d32;">Task Summary</h3>
                <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                    <tr style="background: #2e7d32; color: white;">
                        <th style="padding: 12px; border: 1px solid #ddd; text-align: left;">Task</th>
                        <th style="padding: 12px; border: 1px solid #ddd; text-align: left;">Status</th>
                        <th style="padding: 12px; border: 1px solid #ddd; text-align: left;">Duration</th>
                    </tr>
                    {task_rows}
                </table>

                <p style="margin-top: 20px;">
                    <a href="{log_url}" style="background: #2e7d32; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">View DAG Run</a>
                </p>

                <hr style="margin-top: 30px; border: none; border-top: 1px solid #ddd;">
                <p style="color: #666; font-size: 12px;">
                    This is an automated alert from Apache Airflow.<br>
                    Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </body>
            </html>
            """

            print(f"Sending pipeline success email to: {self.success_email_to}")
            send_email(
                to=self.success_email_to,
                subject=subject,
                html_content=html_content
            )
            print("Pipeline success email sent!")

        except Exception as e:
            print(f"Failed to send success email: {e}")

    