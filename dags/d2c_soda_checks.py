from datetime import datetime, timedelta
import sys
import pandas as pd
from utils.postgresconnector_v3 import PostgresConnector
from airflow.operators.dummy import DummyOperator

sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.shopify.shopify_pnl.load import load_v2
from connections.bsc.shopify.soda_health_check.run_soda import run_scan as run_soda_scan


# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
    
]
# #========================================================================
# Checks definations
# #========================================================================

def data_freshness_checks():
    run_soda_scan(
        "freshness_audit",
        "audit_db",
        ["checks/audit_db/data_freshness_checks.yml"]
    )

def shopify_operational_checks():
    run_soda_scan(
        "freshness_warehouse",
        "warehouse_db",
        ["checks/warehouse_db/shopify_operational_checks.yml"]
    )

def google_spent_checks():
    run_soda_scan(
        "google_spent",
        "warehouse_db",
        ["checks/warehouse_db/google_spent_checks.yml"]
    )

def facebook_spent_checks():
    run_soda_scan(
        "facebook_spent",
        "warehouse_db",
        ["checks/warehouse_db/facebook_spent_checks.yml"]
    )

def ga4_sessions_checks():
    run_soda_scan(
        "ga4_sessions",
        "warehouse_db",
         ["checks/warehouse_db/ga4_sessions_checks.yml"]
    )

def materialized_view_checks():
    run_soda_scan(
        "materialized_views",
        "warehouse_db",
         ["checks/warehouse_db/materialized_view_checks.yml"]
    )



# # =============================================================================
# DASHBOARD REFRESH FUNCTIONS
# # =============================================================================

def refresh_operational_pnl_order_details_view():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""SELECT * FROM bsc.refresh_shopify_order_details(); """
    postgres.execute_query(query)

def refresh_shopify_affiliate_validation_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "REFRESH MATERIALIZED VIEW bsc.shopify_affiliate_validation_v2; "
    postgres.execute_query(query)

def refresh_shopify_marketplace_summary_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "REFRESH MATERIALIZED VIEW bsc.shopify_marketplace_summary_v2; "
    postgres.execute_query(query)

def create_view_shopify_pnl_combined_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""CREATE MATERIALIZED VIEW bsc.shopify_pnl_combined_v2 AS
                        WITH normalized_utm_mapping AS (
                            SELECT DISTINCT ON (
                                UPPER("GA_SOURCEMEDIUM")
                                )
                                UPPER("GA_SOURCEMEDIUM") AS utm_source_medium,
                                UPPER("Final_Channel") AS channel,
                                UPPER("Type") AS type,
                                UPPER("Final_Source") AS source
                            FROM shopify.ga_channel_mapping
                            ORDER BY
                                UPPER("GA_SOURCEMEDIUM")
                        ),

                            combined_ga_data AS (
                                -- BSC Google Analytics data
                                SELECT
                                    UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                    ga."sessions",
                                    ga."totalUsers",
                                    ga.date::date AS date,
                                    'Bombay Shaving Company' AS store
                                FROM bsc.googleanalytics_traffic_sources ga

                                UNION ALL

                                -- Bombae Google Analytics data
                                SELECT
                                    UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                    ga."sessions",
                                    ga."totalUsers",
                                    ga.date::date AS date,
                                    'BOMBAE' AS store
                                FROM bsc.googleanalytics_bae_traffic_sources ga
                            ),

                            ga_data AS (
                                SELECT
                                    ga.date,
                                    ga.store,
                                    CASE
                                        WHEN nutm.type IS NOT NULL THEN nutm.type
                                        ELSE 'CORE'
                                        END as marketingtype,
                                    CASE
                                        WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                        ELSE 'UNPAID'
                                        END as marketingchannel,
                                    CASE
                                        WHEN nutm.source IS NOT NULL THEN nutm.source
                                        ELSE 'DIRECT'
                                        END as marketingsource,
                                    SUM(ga."sessions") as total_sessions
                                FROM combined_ga_data ga
                                        LEFT JOIN normalized_utm_mapping nutm
                                                    ON ga.utm_source_medium = nutm.utm_source_medium
                                GROUP BY
                                    ga.date,
                                    ga.store,
                                    CASE
                                        WHEN nutm.type IS NOT NULL THEN nutm.type
                                        ELSE 'CORE'
                                        END,
                                    CASE
                                        WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                        ELSE 'UNPAID'
                                        END,
                                    CASE
                                        WHEN nutm.source IS NOT NULL THEN nutm.source
                                        ELSE 'DIRECT'
                                        END
                            )

                        SELECT

                            s.orderdate :: date,
                            s.marketingtype,
                            s.marketingsource,
                            s.marketingchannel,
                            s.store AS store,
                            MAX(s.brandname) as brandname,
                            SUM(COALESCE(s.mrpsales,0)) AS mrpsales,
                            SUM(COALESCE(s.grosssales, 0)) AS total_gross,
                            SUM(COALESCE(s.mrpdiscount,0)) AS total_discount,
                            ROUND(SUM(COALESCE(s.grosssales, 0)) - SUM(COALESCE(s.cancelledsales, 0))) AS totalsales_ex_canc,
                            COUNT(DISTINCT CASE
                                            WHEN s.quantity <>0 AND UPPER(s.orderstatus) <>'SHIPPED & RETURNED' OR s.orderstatus is null
                                                THEN s.ordername  END ) AS total_order,
                            SUM(COALESCE(s.quantity, 0))::numeric AS total_quantity,
                            ROUND(SUM(COALESCE(s.allocated_marketing_spend, 0)), 2) AS allocated_marketing_spend,
                            ROUND(SUM(COALESCE(s.affiliatemarketingspend, 0)), 2) AS affiliated_marketing_spend,
                            COUNT(DISTINCT CASE WHEN s.new_customer = 'TRUE' THEN s.phone END) AS new_customer,
                            COUNT(DISTINCT s.phone) 											AS total_customer,
                            COUNT(DISTINCT CASE
                                            WHEN UPPER(s.orderstatus)='CANCELLED'
                                                THEN s.ordername END ) AS canc_orders,
                            ROUND(SUM(COALESCE(s.shipping_price,0)),2) AS shipping_price,
                            ROUND(SUM(COALESCE(s.rtosales,0)),2) AS rtosales,
                            ROUND(SUM(COALESCE(s.tax,0)),2) AS TAX,
                            ROUND(SUM(COALESCE(s.netsales,0)),2) AS netsales,
                            ROUND(SUM(COALESCE(s.cogs,0)),2) AS cogs,
                            ROUND(SUM(COALESCE(s.logisticscost,0)),2) AS logistics,
                            ROUND(SUM(COALESCE(s.cm1,0)),2) AS cm1,
                            ROUND(SUM(COALESCE(s.cm2,0)),2) AS cm2,

                            -- GA sessions from joined table
                            COALESCE(g.total_sessions, 0) AS total_sessions

                        FROM bsc.shopify_operational_pnl_v2 s
                                LEFT JOIN ga_data g ON s.store = g.store
                            AND s.orderdate :: date = g.date
                            AND s.marketingtype = g.marketingtype
                            AND s.marketingchannel = g.marketingchannel
                            AND s.marketingsource = g.marketingsource
                        GROUP BY
                            s.store,
                            s.orderdate :: date,
                            s.marketingtype,
                            s.marketingsource,
                            s.marketingchannel,
                            total_sessions
                        ORDER BY s.orderdate :: date DESC, total_gross DESC;"""
    create_or_refresh_view('shopify_pnl_combined_v2', query)


def create_view_shopify_sales_analysisV2_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""
    
    CREATE MATERIALIZED VIEW bsc.shopify_sales_analysisV2_v2 AS
                    WITH normalized_utm_mapping AS (
                        SELECT DISTINCT ON (
                            UPPER("GA_SOURCEMEDIUM")
                            )
                            UPPER("GA_SOURCEMEDIUM") AS utm_source_medium,
                            UPPER("Final_Channel") AS channel,
                            UPPER("Type") AS type,
                            UPPER("Final_Source") AS source
                        FROM shopify.ga_channel_mapping
                        ORDER BY
                            UPPER("GA_SOURCEMEDIUM")
                    ),

                        combined_ga_data AS (
                            -- BSC Google Analytics data
                            SELECT
                                UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                ga."engagedSessions",
                                ga."totalUsers",
                                ga.date::date AS date,
                                'Bombay Shaving Company' AS store
                            FROM bsc.googleanalytics_traffic_acquisition_session_source_medium_repor ga

                            UNION ALL

                            -- Bombae Google Analytics data
                            SELECT
                                UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                ga."engagedSessions",
                                ga."totalUsers",
                                ga.date::date AS date,
                                'BOMBAE' AS store
                            FROM bsc.googleanalytics_bae_traffic_acquisition_session_source_medium_r ga
                        ),

                        ga_data AS (
                            SELECT
                                ga.date,
                                ga.store,
                                CASE
                                    WHEN nutm.type IS NOT NULL THEN nutm.type
                                    ELSE 'CORE'
                                    END as marketingtype,
                                CASE
                                    WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                    ELSE 'UNPAID'
                                    END as marketingchannel,
                                CASE
                                    WHEN nutm.source IS NOT NULL THEN nutm.source
                                    ELSE 'DIRECT'
                                    END as marketingsource,
                                SUM(ga."engagedSessions") as total_sessions
                            FROM combined_ga_data ga
                                    LEFT JOIN normalized_utm_mapping nutm
                                                ON ga.utm_source_medium = nutm.utm_source_medium
                            GROUP BY
                                ga.date,
                                ga.store,
                                CASE
                                    WHEN nutm.type IS NOT NULL THEN nutm.type
                                    ELSE 'CORE'
                                    END,
                                CASE
                                    WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                    ELSE 'UNPAID'
                                    END,
                                CASE
                                    WHEN nutm.source IS NOT NULL THEN nutm.source
                                    ELSE 'DIRECT'
                                    END
                        )

                    SELECT

                        s.orderdate :: date,
                        s.marketingtype,
                        s.marketingsource,
                        s.marketingchannel,
                        s.store AS store,
                        MAX(s.brandname) as brandname,
                        SUM(COALESCE(s.grosssales, 0)) AS total_gross,
                        SUM(COALESCE(s.mrpdiscount,0)) AS total_discount,
                        ROUND(SUM(COALESCE(s.grosssales, 0)) - SUM(COALESCE(s.cancelledsales, 0))) AS totalsales_ex_canc,
                        COUNT(DISTINCT CASE
                                        WHEN s.quantity <>0 AND UPPER(s.orderstatus) <>'SHIPPED & RETURNED' OR s.orderstatus is null
                                            THEN s.ordername  END ) AS total_order,
                        SUM(COALESCE(s.quantity, 0))::numeric AS total_quantity,
                        ROUND(SUM(COALESCE(s.allocated_marketing_spend, 0)), 2) AS allocated_marketing_spend,
                        ROUND(SUM(COALESCE(s.affiliatemarketingspend, 0)), 2) AS affiliated_marketing_spend,
                        COUNT(DISTINCT CASE WHEN s.new_customer = 'TRUE' THEN s.phone END) AS new_customer,
                        COUNT(DISTINCT s.phone) 											AS total_customer,
                        COUNT(DISTINCT CASE
                                        WHEN UPPER(s.orderstatus)='CANCELLED'
                                            THEN s.ordername END ) AS canc_orders,
                        SUM(COALESCE(CASE
                                        WHEN UPPER(s.orderstatus) = 'CANCELLED' THEN s.quantity ELSE 0 END ,0)) AS canc_quantity  ,

                        -- GA sessions from joined table
                        COALESCE(g.total_sessions, 0) AS total_sessions

                    FROM bsc.shopify_operational_pnl_v2 s
                            LEFT JOIN ga_data g ON s.store = g.store
                        AND s.orderdate :: date = g.date
                        AND s.marketingtype = g.marketingtype
                        AND s.marketingchannel = g.marketingchannel
                        AND s.marketingsource = g.marketingsource
                    GROUP BY
                        s.store,
                        s.orderdate :: date,
                        s.marketingtype,
                        s.marketingsource,
                        s.marketingchannel,
                        total_sessions
                    ORDER BY s.orderdate :: date DESC, total_gross DESC;
                    
                    """
    create_or_refresh_view('shopify_sales_analysisV2_v2', query)


def refresh_ads_performance_summary_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """BEGIN ;
            SET max_parallel_workers_per_gather = 0;
            REFRESH MATERIALIZED VIEW bsc.meta_performancev2;
            REFRESH MATERIALIZED VIEW bsc.google_performance;
            REFRESH MATERIALIZED VIEW bsc.gm_data;
            COMMIT;"""
    postgres.execute_query(query)

def create_or_refresh_view(view_name, create_query):
    postgres = PostgresConnector(db_prefix="warehouse_")
    
    combined_query = f"""
        SET max_parallel_workers_per_gather = 0;
        
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_matviews 
                WHERE schemaname = 'bsc' 
                AND matviewname = '{view_name.lower()}'
            ) THEN
                REFRESH MATERIALIZED VIEW bsc.{view_name};
            ELSE
                {create_query}
            END IF;
        END $$;
        
        RESET max_parallel_workers_per_gather;
    """
    
    postgres.execute_query(combined_query)





# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='d2c_soda_checks',
    description='D2C pipeline health check BSC',
    schedule_interval='30 1 * * *',  # Daily at 7 AM IST
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    catchup=False,
    tags=['etl', 'shopify', 'pnl', 'health-check'],
)

# End marker  
end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule='none_failed_min_one_success',  # Continue even if some tasks fails
    dag=dag
)

# # =============================================================================
# # SODA CHECK TASKS
# # =============================================================================


data_freshness_task = TrackedPythonOperator(
    task_id='data_freshness_checks',
    python_callable=data_freshness_checks,
    pipeline_name='d2c-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

shopify_operational_task = TrackedPythonOperator(
    task_id='shopify_operational_checks',
    python_callable=shopify_operational_checks,
    pipeline_name='d2c-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

google_spent_task = TrackedPythonOperator(
    task_id='google_spent_checks',
    python_callable=google_spent_checks,
    pipeline_name='d2c-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

facebook_spent_task = TrackedPythonOperator(
    task_id='facebook_spent_checks',
    python_callable=facebook_spent_checks,
    pipeline_name='d2c-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    on_task_failure_callable=lambda: load_v2(day=5),
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)


ga4_sessions_task = TrackedPythonOperator(
    task_id='ga4_sessions_checks',
    python_callable=ga4_sessions_checks,
    pipeline_name='d2c-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

materialized_view_task = TrackedPythonOperator(
    task_id='materialized_view_checks',
    python_callable=materialized_view_checks,
    pipeline_name='d2c-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)


# # =============================================================================
# # DASHBOARD REFRESH TASKS
# # =============================================================================

refresh_operational_pnl_order_details_view = TrackedPythonOperator(
    task_id='refresh_operational_pnl_order_details_view',
    python_callable=refresh_operational_pnl_order_details_view,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)


refresh_shopify_affiliate_validation_v2 = TrackedPythonOperator(
    task_id='refresh_shopify_affiliate_validation_v2',
    python_callable=refresh_shopify_affiliate_validation_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

refresh_shopify_marketplace_summary_v2 = TrackedPythonOperator(
    task_id='refresh_shopify_marketplace_summary_v2',
    python_callable=refresh_shopify_marketplace_summary_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

create_view_shopify_pnl_combined_v2 = TrackedPythonOperator(
    task_id='create_view_shopify_pnl_combined_v2',
    python_callable=create_view_shopify_pnl_combined_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

create_view_shopify_sales_analysisV2_v2 = TrackedPythonOperator(
    task_id='create_view_shopify_sales_analysisV2_v2',
    python_callable=create_view_shopify_sales_analysisV2_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

refresh_view_shopify_ads_performance_summary_v2 = TrackedPythonOperator(
    task_id='refresh_view_shopify_ads_performance_summary_v2',
    python_callable=refresh_ads_performance_summary_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)


data_freshness_task >> shopify_operational_task >> google_spent_task >> facebook_spent_task >> ga4_sessions_task >> materialized_view_task 

materialized_view_task >> refresh_operational_pnl_order_details_view >> refresh_shopify_affiliate_validation_v2 >> refresh_shopify_marketplace_summary_v2 

refresh_shopify_marketplace_summary_v2 >> create_view_shopify_pnl_combined_v2 >> create_view_shopify_sales_analysisV2_v2 >> refresh_view_shopify_ads_performance_summary_v2 >> end_task
