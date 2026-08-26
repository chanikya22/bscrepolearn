"""
Airflow DAG: one DAG per WORKBOOK -- standard pattern for a workbook with
several sheets you want synced, instead of one DAG per sheet.

Each SheetSyncConfig below becomes its own task in this single DAG. Note
site_id and file_path repeat across entries -- that's expected, since every
sheet lives in the same physical workbook.
"""
from custom_packages.excel_sync import SheetSyncConfig, SyncMode, create_workbook_sync_dag

WORKBOOK_SITE_ID = "vlpcpl.sharepoint.com,36c1362d-c807-4966-b80b-e8cfe7e05874,5e5d1942-a7c3-405c-b252-90cef59a085a"
WORKBOOK_FILE_PATH = "/PriceMonitoring.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="SKU List",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="market_intelligence",
        target_table="SKU List",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="price_monitoring_sku_details",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="market_intelligence",
        target_table="price_monitoring_sku_details",
    )
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_price_monitoring_multi_sheet_test",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)


################--Neurogum QCOM Product Master--####################
WORKBOOK_FILE_PATH = "/100Days/Neurogum/neurogum_qcom_productmaster.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="Blinkit Product Master",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="Blinkit_Product_Master",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="Swiggy Product Master",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="Swiggy_Product_Master",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="Zepto Product Master",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="Zepto_Product_Master",
    ),
        SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="Swiggy Combo Product Master",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="Swiggy_Combo_Product_Master",
    )
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_neurogum_qcom_productmaster",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)



####################################--Modenik Enamor UTM Mapping--####################################
WORKBOOK_FILE_PATH = "/100Days/Modenik/EnamorUTM_Mapping.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="utm_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="enamor",
        target_table="utm_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="discount_code_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="enamor",
        target_table="discount_code_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="commission",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="enamor",
        target_table="commission",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="ga_channel_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="enamor",
        target_table="ga_channel_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="productmaster",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="enamor",
        target_table="productmaster",
    )
    # SheetSyncConfig(
    #     site_id=WORKBOOK_SITE_ID,
    #     file_path=WORKBOOK_FILE_PATH,
    #     sheet_name="Product master 2",
    #     sync_mode=SyncMode.FULL_SHEET,
    #     target_schema="enamor",
    #     target_table="",
    # ),
    # SheetSyncConfig(
    #     site_id=WORKBOOK_SITE_ID,
    #     file_path=WORKBOOK_FILE_PATH,
    #     sheet_name="Format Required",
    #     sync_mode=SyncMode.FULL_SHEET,
    #     target_schema="enamor",
    #     target_table="",
    # ),
]

dag_enamor_utm_mapping = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_EnamorUtm_Mapping",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)



##################################--Dfm qcom product master--##################################
WORKBOOK_FILE_PATH = "/100Days/DFM/dfm_qcom_productmaster.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="blinkit_productmaster",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="dfm",
        target_table="blinkit_productmaster",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="swiggy_productmaster",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="dfm",
        target_table="swiggy_productmaster",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="zepto_productmaster",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="dfm",
        target_table="zepto_productmaster",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="blinkit_city_zone",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="dfm",
        target_table="blinkit_city_zone",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="swiggy_city_zone",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="dfm",
        target_table="swiggy_city_zone",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="zepto_city_zone",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="dfm",
        target_table="zepto_city_zone",
    ),
#         SheetSyncConfig(
#         site_id=WORKBOOK_SITE_ID,
#         file_path=WORKBOOK_FILE_PATH,
#         sheet_name="sheet9",
#         sync_mode=SyncMode.FULL_SHEET,
#         target_schema="dfm",
#         target_table="",
#     ),
#         SheetSyncConfig(
#         site_id=WORKBOOK_SITE_ID,
#         file_path=WORKBOOK_FILE_PATH,
#         sheet_name="helper",
#         sync_mode=SyncMode.FULL_SHEET,
#         target_schema="dfm",
#         target_table="",
#     ),
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_dfm_qcom_productmaster",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)

####################################--Modenik QCOM Product Master--####################################

WORKBOOK_FILE_PATH = "/100Days/Modenik/modenik_qcom_productmaster.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="blinkit_productmaster",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="blinkit_productmaster",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="swiggy_productmaster",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="swiggy_productmaster",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="zepto_productmaster",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="zepto_productmaster",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="blinkit_city_zone",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="blinkit_city_zone",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="swiggy_city_zone",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="swiggy_city_zone",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="zepto_city_zone",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="zepto_city_zone",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="blinkit_target",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="blinkit_target",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="swiggy_target",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="swiggy_target",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="zepto_target",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="modenik",
        target_table="zepto_target",
    ),
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_modenik_qcom_productmaster",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)


#########################-Neurogum Shopify UTM Mapping--####################################
WORKBOOK_FILE_PATH = "/100Days/Neurogum/neurogum shopify utm mapping.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="utm_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="utm_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="discount_code_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="discount_code_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="commission",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="commission",
    ),
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_neurogum_shopify_utm_mapping",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)

#########################--100Days Spend--####################################
WORKBOOK_FILE_PATH = "/100Days/100Days_Spend.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="spend_avon",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="avon",
        target_table="spend_avon",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="spend_neurogum",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="neurogum",
        target_table="spend_neurogum",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="spend_enamor",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="enamor",
        target_table="spend_enamor",
    ),
    # # SheetSyncConfig(
    # #     site_id=WORKBOOK_SITE_ID,
    # #     file_path=WORKBOOK_FILE_PATH,
    # #     sheet_name="spend_ryze",
    # #     sync_mode=SyncMode.FULL_SHEET,
    # #     target_schema="ryze",
    # #     target_table="spend_ryze",
    # ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="durex",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="durex",
        target_table="durex",
    ),
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_100days_spend",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)

#########################---D2C Shopify UTM Mapping--####################################

WORKBOOK_FILE_PATH = "/D2C/UTM Mappings.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="utm_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="shopify",
        target_table="utm_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="discount_code_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="shopify",
        target_table="discount_code_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="commission",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="shopify",
        target_table="commission",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="ga_channel_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="shopify",
        target_table="ga_channel_mapping",
    ),
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_shopify_utm_mappings",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)



###################--Lead time mapping--###################

WORKBOOK_FILE_PATH = "/Lead Time Mapping.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="whsku_leadtime_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="whsku_leadtime_mapping",
    ),
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_bsc_lead_time_mapping",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)


###################--Insyt Automation--###################  
WORKBOOK_FILE_PATH = "/InsytAutomation.xlsx"

sheets = [
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="amazon_sku_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="amazon_sku_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="blinkit_locality_mapping",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="blinkit_locality_mapping",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="flipkart_anchor_prices",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="flipkart_anchor_prices",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="myntra_margins",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="myntra_margins",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="nykaa_margins",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="nykaa_margins",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="swiggy_master_variables",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="swiggy_master_variables",
    ),
    SheetSyncConfig(
        site_id=WORKBOOK_SITE_ID,
        file_path=WORKBOOK_FILE_PATH,
        sheet_name="zepto_master_variables",
        sync_mode=SyncMode.FULL_SHEET,
        target_schema="bsc",
        target_table="zepto_master_variables",
    ),
]

dag = create_workbook_sync_dag(
    sheets,
    dag_id="sync_excel_bsc_insyt_automation",
    schedule="0 6 * * *",
    max_active_tasks=3,  # caps concurrent Graph API calls from this workbook's sheets
)