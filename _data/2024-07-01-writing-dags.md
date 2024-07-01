---
layout: post
title: Writing DAGS.
date: 2024-07-01 10:22
categories: [data_engineering,]
description: A post about writing dags in Airflow.
---

# A Little DAG I Wrote the Other Day

A dag I wrote the other day:

```python
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import oracledb, os, configparser
from dotenv import dotenv_values
from ast import literal_eval
import pandas as pd
import os, json, configparser
from collections import OrderedDict
from datetime import datetime, date
from jinja2 import Environment, FileSystemLoader

def create_config_environment(home='/home/earl'):
    global home_dir, data_dir, templates_dir, docs_dir
    global db_host, db_port, db_lib
    global secret_path, username, password, dsn
    
    config = configparser.ConfigParser()
    config_path = os.path.join(home, 'config.ini')
    config.read(config_path)
    
    home_dir = config.get('Environment', 'home_dir')
    data_dir = config.get('Environment', 'data_dir')
    templates_dir = config.get('Environment', 'templates_dir')
    docs_dir = config.get('Environment', 'docs_dir')
    secret_dir = config.get('Environment', 'secret_dir')
    
    db_host = config.get('Database', 'db_host')
    db_port = config.get('Database', 'db_port')
    db_lib = literal_eval(config.get('Database', 'db_lib'))
    secret_path = os.path.join(secret_dir, '.env')
    
    secret = dotenv_values(secret_path)
    username = secret['username']
    password = secret['password']
    dsn = f'(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(Host={db_host})(Port={db_port})))(CONNECT_DATA=(SID=XE)))'
    oracledb.defaults.fetch_lobs = False
    oracledb.init_oracle_client(lib_dir=db_lib)
    return None

create_config_environment()

def get_dataframe(sql_query):
    try:
        with oracledb.connect(user=username, password=password, dsn=dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                return [row for row in cursor]
    except oracledb.Error as error:
        return [error, None]

def parse_col(col):
    try:
        col = eval(col.strip().split(" ", maxsplit=1)[1])
    except IndexError as e:
        col = col.split('.')[1].replace('_', ' ').title()
    except NameError as e:
        col = col.strip().split(" ", maxsplit=1)[1]
    except Exception as e:
        raise(f"General Exception: {e}")
    return col

def columns(sql_query):
    col_names = sql_query.rsplit("FROM", 1)[0].split('SELECT')[1].replace("\n", "").split(",")
    col_names = [x.strip() for x in col_names]
    col_names = [parse_col(x) for x in col_names if x] if len(col_names) > 1 else col_names
    return col_names

###############################################################################
# Atomic Tasks
###############################################################################

def create_queries(order_no='160016214'):
    order_no = order_no
    table = f'''
    SELECT
        b.ORDER_NO "Order No",
        a.ORDER_ID "Order Id",
        a.OPER_KEY "Oper Key",
        a.STEP_KEY "Step Key"
    FROM 
        sfmfg.sfwid_serial_oper_dat_col a
        LEFT JOIN sfmfg.sfwid_order_desc b ON a.ORDER_ID = b.ORDER_ID
    WHERE
        b.ORDER_NO = '{order_no}'
    '''
    val = table.replace(";", "").replace('from', 'FROM').replace('select', 'SELECT')
    try:
        df = get_dataframe(val)[0]
    except oracledb.Error as e:
        raise(e)
    except IndexError as e: 
        print(f'No data found for {order_no}')
        return None
    except Exception as e:
        raise(e)
    if len(df) <= 0:
        print(f'No data found for {order_no}')
        return None
    cols = columns(val)
    data_dict = {key:val for key, val in zip(cols, df)}

    ORDER_NO, ORDER_ID, OPER_KEY, STEP_KEY = (data_dict['Order No'], data_dict['Order Id'], data_dict['Oper Key'], data_dict['Step Key'])

    query_dict = OrderedDict({
        'WOHdrQuery': f'''
    SELECT
        a.ORDER_NO "Order No",
        a.PLAN_TITLE "Order Title",
        a.ORDER_QTY "Order Qty",
        a.ORDER_UOM "UOM",
        a.ASGND_WORK_LOC "Main Work Location",
        a.ORDER_STATUS "Order Status",
        c.UCF_MBOM_REV_VCH1 "Configuration",
        b.PLAN_NO "Plan No",
        a.PLAN_VERSION "Plan Ver",
        a.PLAN_REVISION "Plan Rev",
        a.PLAN_TYPE "Plan Type",
        a.PART_NO "Plan Item No",
        a.PART_CHG "Plan Item Rev",
        a.ITEM_TYPE "Plan Item Type",
        a.ITEM_SUBTYPE "Plan Item Subtype",
        a.MODEL "Model",
        a.ORDER_CUST_ID "Order Cust Id",
        a.PROJECT "Project",
        a.PROGRAM "Program",
        a.UCF_PLAN_VCH2 "Route Type",
        a.UNIT_NO "End Unit No",
        a.UNIT_TYPE "End Unit Type",
        a.LTA_SEND_FLAG "Labor Send Flag",
        a.UCF_PLAN_VCH1 "Lot Qty",
        a.LOT_FLAG "Lot No?",
        a.SERIAL_FLAG "Serial No?",
        a.SPLIT_FLAG "Was Split?",
        a.ORDER_COMPLETE_QTY "Complete Qty",
        a.ORDER_STOP_QTY "Stop Qty",
        a.ORDER_SCRAP_QTY "Scrap Qty",
        a.CUSTOMER_ORDER_NO "Customer Order No",
        a.INITIAL_STORES "Initial Stores",
        a.FINAL_STORES "Final Stores",
        a.SCHED_PRIORITY "Sched Priority",
        a.SCHED_START_DATE "Sched Start Date",
        a.SCHED_END_DATE "Sched End Date",
        a.ACTUAL_START_DATE "Actual Start Date",
        a.ACTUAL_END_DATE "Actual End Date",
        a.REVISED_START_DATE "Revised Start Date",
        a.REVISED_END_DATE "Revised End Date",
        a.CONTRACT_NO "Contract No",
        a.CONDITION "Condition",
        a.UID_ITEM_FLAG "UID Item Flag",
        a.UID_ENTRY_NAME "UID Entry Name",
        a.FAI_OPER_REQD "FAI Oper Reqd",
        a.ALT_COUNT "Alterations"
        
    FROM
        sfmfg.sfwid_order_desc a
        left outer join sfmfg.sfpl_plan_desc b 
        on a.PLAN_ID=b.PLAN_ID 
        left outer join sfmfg.SFPL_MFG_BOM_REV c 
        on a.BOM_NO=c.BOM_NO and a.MFG_BOM_CHG=c.MFG_BOM_CHG;
    where a.order_no='{ORDER_NO}';
    ''',

        'serialsQuery': f'''

    SELECT 
        PART_NO "Item No" ,
        LOT_NO "Lot No",
        SERIAL_NO "Serial No",
        SERIAL_STATUS "Order Status",
        SERIAL_HOLD_STATUS "Hold Status"
    FROM
        sfmfg.SFWID_PARTS_LOTS_SERIALS 
    where ORDER_ID='{ORDER_ID}' order by LOT_NO, SERIAL_NO;
    ''',
        'text query': f'''
    select 
        ORDER_ID "Order Id",
        text "Text"
    from 
        sfmfg.SFWID_ORDER_TEXT 
    WHERE ORDER_ID = '{ORDER_ID}';
        --order by text_type
        --where  ORDER_ID= '{ORDER_ID}' order by text_type;
        --where ORDER_ID='" & Order_ID.Replace("'", "''") & "' order by text_type;
    ''',
        'operTextQuery': f'''
        SELECT
            Z.ORDER_ID "Order Id",
            Z.OPER_KEY "Oper Key",
            Z.OPER_NO "Oper No",
            Z.STEP_KEY "Step Key",
            Z.STEP_NO "Step No",
            A.TEXT Text,
            A.TEXT_TYPE "Text Type"
        FROM
            SFMFG.SFWID_OPER_DESC Z
            FULL OUTER JOIN SFMFG.SFWID_OPER_TEXT A on
                Z.ORDER_ID=A.ORDER_ID
                AND Z.OPER_KEY=A.OPER_KEY
                AND Z.STEP_KEY=A.STEP_KEY
        WHERE
            '{ORDER_ID}'=Z.ORDER_ID
        ''',
        'itemQuery': f'''

    SELECT

        a.ORDER_NO "Order No",
        b.PART_NO "Part No",
        b.PART_CHG "Part Rev",
        b.PART_ACTION "Part Action",
        b.PART_TITLE "Part Title",
        c.ITEM_SUBTYPE "Part Subtype",
        b.REF_DES "Ref Des",
        b.REF_DES_PREF_RANK "Ref Des Pref Rank",
        b.STOCK_UOM "UOM",
        c.PLND_ITEM_QTY "Qty",
        b.FIND_NO "Find No",
        b.OPTIONAL_FLAG "Optional?",
        b.SERIAL_FLAG "Serial No",
        b.LOT_FLAG "Lot No?",
        b.EXP_FLAG "Exp Date?",
        c.SPOOL_FLAG "Spool No?",
        c.UID_ITEM_FLAG "UID Item Flag",
        c.UID_ENTRY_NAME "UID Entry Name",
        c.ORIENTATION_FLAG "Orientation",
        c.CROSS_ORDER_FLAG "Cross Order?",
        --b.PLND_ITEM_ID "Plnd Item ID",
        b.OPER_NO "Oper No",
        b.STEP_NO "Step No",
        b.OPER_KEY "Oper Key",
        b.STEP_KEY "Step Key",
        b.ITEM_NOTES "Item Notes",
        b.OVER_CONSUMPTION_FLAG "Over Consumption Flag",
        c.STANDARD_PART_FLAG "Standard Part Flag",
        c.ITEM_CATEGORY "Item Category",
        c.STORE_LOC "Store Loc",
        c.UNLOADING_POINT "Unloading Point",
        c.UPDT_USERID "Updt Userid",
        c.TIME_STAMP "Time Stamp",
        --b.REF_ID "Ref Id",
        --b.BLOCK_ID "Block Id",
        b.SUSPECT_FLAG "Suspect Flag",
        c.EXTERNAL_PLM_NO "External Plm No",
        c.EXTERNAL_ERP_NO "External Erp No",
        --c.SLIDE_ID "Slide Id",
        --c.SLIDE_EMBEDDED_REF_ID "Slide Embedded Ref Id",
        c.REMOVE_ACTION "Remove Action",
        c.UTILIZATION_RULE "Utilization Rule",
        c.TRACKABLE_FLAG "Trackable Flag", 
        --c.PART_DAT_COL_ID "Part Dat Col Id",
        c.POSITION_NO "Position No",
        c.UNIT_TYPE "Unit Type",
        c.EFF_FROM "Eff From",
        c.EFF_THRU "Eff Thru",
        c.EFF_FROM_DATE "Eff From Date", 
        c.EFF_THRU_DATE "Eff Thru Date",
        a.ITEM_TYPE "Item Type",
        c.SECURITY_GROUP "Security Group"
        
    from 
        SFMFG.sfwid_order_desc a left outer join
        SFMFG.SFWID_SERIAL_OPER_PART_V b
        on a.Order_ID = b. Order_ID 
        left outer join SFMFG.SFWID_OPER_ITEMS c
        ON a.Order_ID =c.Order_ID
    where a.order_id='{ORDER_ID}' 
    -- order by OPER_KEY, STEP_KEY, REF_ID;
    ''',
        'itemSerialQuery': f'''
    select 
        -- a.ORDER_ID "Order Id",
        -- a.OPER_KEY "Oper Key",
        -- a.STEP_KEY "Step Key",
        -- a.LOT_ID "Lot Id",
        -- a.SERIAL_ID "Serial Id",
        -- a.PART_DAT_COL_ID "Part Dat Col Id",
        -- a.PLND_ITEM_ID "Plnd Item Id",
        a.REF_DES "Ref Des",
        a.PART_NO "Part No",
        a.PART_CHG "Part Chg",
        a.UPDT_USERID "Updt Userid",
        a.TIME_STAMP "Time Stamp",
        a.LAST_ACTION "Last Action",
        a.ITEM_DAT_COL_STATUS "Status",
        a.OPER_ITERATION "Oper Iteration",
        a.OPER_EXE_COUNT "Oper Exe Count",
        b.SERIAL_NO "Serial No",
        b.LOT_NO "Lot No"
    from 
        sfmfg.sfwid_serial_oper_items a left outer join sfmfg.SFWID_AS_WORKED_BOM b
        on a.ORDER_ID=b.ORDER_ID 
        and a.LOT_ID=b.PARENT_LOT_ID 
        and a.SERIAL_ID=b.PARENT_SERIAL_ID 
        and a.OPER_KEY=b.OPER_KEY 
        and a.STEP_KEY=b.STEP_KEY 
        and a.PART_DAT_COL_ID=b.PART_DAT_COL_ID 
    where a.order_id='{ORDER_ID}';
    ''',
        'toolQuery': f'''
    SELECT 
        a.ORDER_ID "Order Id",
        a.OPER_KEY "Oper Key",
        a.STEP_KEY "Step Key",
        a.TOOL_NO "Tool No",
        a.TOOL_CHG "Tool Rev",
        a.QTY "Qty",
        a.TOOL_TITLE "Tool Title",
        a.SERIAL_FLAG "Serial No?",
        a.SERIAL_KITTED "Serial Kitted",
        a.EXP_FLAG "Exp Date?",
        a.OPTIONAL_FLAG "Optional?",
        a.ORIENTATION_FLAG "Orientation",
        a.CROSS_ORDER_FLAG "Cross Order?",
        a.UCF_PLAN_TOOL_VCH1 "Company",
        a.UCF_PLAN_TOOL_VCH2 "Gage Type",
        a.STEP_NO "Step No",
        a.OPER_NO "Oper No",
        a.UPDT_USERID "Updt Userid",
        a.TIME_STAMP "Time Stamp",
        a.LAST_ACTION "Last Action",
        a.TOOL_NOTES "Tool Notes",
        a.MANUFACTURER "Manufacturer",
        a.TOOL_MODEL "Tool Model",
        -- a.ALT_ID "Alt Id",
        a.ALT_COUNT "Alterations",
        -- a.UCF_PLAN_TOOL_VCH3 "Ucf Plan Tool Vch3",
        -- a.UCF_PLAN_TOOL_FLAG1 "Ucf Plan Tool Flag1",
        -- a.UCF_PLAN_TOOL_DATE1 "Ucf Plan Tool Date1",
        -- a.UCF_PLAN_TOOL_NUM1 "Ucf Plan Tool Num1",
        -- a.REF_ID "Ref Id",
        -- a.BLOCK_ID "Block Id",
        -- a.SUSPECT_FLAG "Suspect Flag",
        a.EXTERNAL_PLM_NO "External Plm No",
        a.EXTERNAL_ERP_NO "External Erp No",
        -- a.SLIDE_EMBEDDED_REF_ID "Slide Embedded Ref Id",
        -- a.SLIDE_ID "Slide Id",
        a.IS_TOOL_KITTED "Is Tool Kitted",
        a.ITEM_TYPE "Item Type",
        a.ITEM_SUBTYPE "Item Subtype",
        a.SECURITY_GROUP "Security Group"
    from 
        sfmfg.sfwid_oper_Tool a
    where order_id='{ORDER_ID}' 
    -- order by OPER_KEY, STEP_KEY, REF_ID;
    ''', 'toolSerialQuery': f'''

    select 
        a.ORDER_ID "Order Id",
        a.OPER_KEY "Oper Key",
        a.STEP_KEY "Step Key",
        a.LOT_ID "Lot Id",
        a.SERIAL_ID "Serial Id",
        a.ASGND_TOOL_NO "Assigned Tool No",
        a.ASGND_TOOL_CHG "Assigned Tool Rev",
        -- a.UPDT_USERID "Updt Userid",
        -- a.TIME_STAMP "Time Stamp",
        -- a.LAST_ACTION "Last Action",
        a.PLND_TOOL_NO "Plnd Tool No",
        a.PLND_TOOL_CHG "Plnd Tool Chg",
        a.ASGND_TOOL_QTY "Assigned Qty", 
        a.TOOL_DAT_COL_STATUS "Status",
        a.OPER_ITERATION "Oper Iteration",
        a.OPER_EXE_COUNT "Oper Exe Count"

    from 
        sfmfg.sfwid_serial_oper_tool a
    where order_id='{ORDER_ID}';
    ''', 'Tools': f'''
    SELECT 
        a.OPER_KEY "Oper Key",
        a.STEP_KEY "Step Key",
        a.TOOL_NO "Tool No",
        a.TOOL_CHG "Tool Rev",
        a.QTY "Qty",
        a.TOOL_TITLE "Tool Title",
        a.SERIAL_FLAG "Serial No?",
        a.SERIAL_KITTED "Serial Kitted",
        a.EXP_FLAG "Exp Date?",
        a.OPTIONAL_FLAG "Optional?",
        a.ORIENTATION_FLAG "Orientation",
        a.CROSS_ORDER_FLAG "Cross Order?",
        a.UCF_PLAN_TOOL_VCH1 "Company",
        a.UCF_PLAN_TOOL_VCH2 "Gage Type",
        a.ALT_COUNT "Alterations",  
        b.ASGND_TOOL_NO "Assigned Tool No",
        b.ASGND_TOOL_CHG "Assigned Tool Rev",
        b.ASGND_TOOL_QTY "Assigned Qty", 
        b.TOOL_DAT_COL_STATUS "Status"
    from 
        sfmfg.sfwid_oper_Tool a
        left join sfmfg.sfwid_serial_oper_tool b
        on a.ORDER_ID = b.ORDER_ID
    where a.order_id='{ORDER_ID}' 
    order by a.OPER_KEY, a.STEP_KEY
        --, REF_ID;
    ''',
        'dcQuery': f'''
    select 
        a.ORDER_ID "Order Id",
        a.OPER_KEY "Oper Key",
        a.STEP_KEY "Step Key",
        a.DAT_COL_ID "Dat Col Id", 
        a.OPER_NO "Oper No",
        a.STEP_NO "Step No",
        a.DAT_COL_TITLE "Data Collection Title",
        a.DAT_COL_TYPE "Data Collection Type",
        a.DAT_COL_UOM "UOM",
        a.LOWER_LIMIT "Lower Limit",
        a.TARGET_VALUE "Target Value",
        a.UPPER_LIMIT "Upper Limit",
        a.DAT_COL_CERT "Required Cert",
        a.OPTIONAL_FLAG "Optional?",
        a.UCF_OPER_DC_FLAG1 "FAI",
        a.UCF_OPER_DC_NUM1 "Seq",
        a.UCF_OPER_DC_NUM2 "Ref",
        a.UCF_OPER_DC_VCH3 "Zone",
        a.UCF_OPER_DC_VCH1 "KPC ID",
        a.UCF_OPER_DC_VCH2 "KPC Type",
        a.UCF_OPER_DC_FLAG2 "RD",
        a.ALT_COUNT "Alterations",
        -- a.ALT_ID "Alt ID",
        -- a.UPDT_USERID "Updt Userid",
        -- a.TIME_STAMP "Time Stamp",
        -- a.LAST_ACTION "Last Action",
        -- a.BLOCK_ID "Block Id",
        -- a.REF_ID "Ref Id",
        -- a.DISPLAY_LINE_NO "Display Line No",
        a.ORIENTATION_FLAG "Orientation Flag",
        a.CROSS_ORDER_FLAG "Cross Order Flag",
        a.VARIABLE_NAME "Variable Name",
        a.VISIBILITY "Visibility",
        a.NUM_DECIMAL_DIGITS "Num Decimal Digits",
        a.CALC_DC_FLAG "Calc Dc Flag"
        -- a.SUSPECT_FLAG "Suspect Flag",
        -- a.SLIDE_EMBEDDED_REF_ID "Slide Embedded Ref Id",
        -- a.SLIDE_ID "Slide Id"

    from 
        sfmfg.sfwid_oper_dat_col a
    where a.order_id='{ORDER_ID}' 
    --order by OPER_KEY, STEP_KEY, REF_ID, DISPLAY_LINE_NO;
    ''',
        'dcSerialQuery': f'''
    select 
        a.ORDER_ID "Order Id",
        a.OPER_KEY "Oper Key",
        a.STEP_KEY "Step Key",
        a.LOT_ID "Lot Id",
        a.SERIAL_ID "Serial Id",
        a.DAT_COL_ID "Dat Col Id",
        a.UPDT_USERID "Updt Userid", 
        a.TIME_STAMP "Time Stamp", 
        a.LAST_ACTION "Last Action", 
        a.DCVALUE "Data Value", 
        a.COMMENTS "Comments", 
        a.OPER_ITERATION "Oper Iteration", 
        a.OPER_EXE_COUNT "Oper Exe Count", 
        a.XBAR_CPU_VALUE "Xbar Cpu Value", 
        a.XBAR_CPL_VALUE "Xbar Cpl Value", 
        a.XBAR_CPK_INDEX "Xbar Cpk Index", 
        a.XBAR_EST_SIGMA_VALUE "Xbar Est Sigma Value", 
        a.DP_SUBGROUP_SIZE "Dp Subgroup Size", 
        a.OOC_CAUSE_FLAG  "Ooc Cause Flag",
        a.OOC_REASON_NUM  "Ooc Reason Num", 
        a.OUTLIER_FLAG "Outlier Flag"
    from 
        sfmfg.sfwid_serial_oper_dat_col a 
    where a.order_id='{ORDER_ID}';
        --where a.order_id=':ORDER_ID' and a.oper_key=':OPER_KEY' and a.step_key=':STEP_KEY' and a.dat_col_id=':DAT_COL_ID' order by LOT_NO, SERIAL_NO;
    ''',
        'dcQuery and dcSerialQuery combined': f'''
    SELECT
        a.DAT_COL_TITLE "Data Collection Title",
        a.DAT_COL_TYPE "Data Collection Type",
        a.DAT_COL_UOM "UOM",
        a.LOWER_LIMIT "Lower Limit",
        a.TARGET_VALUE "Target Value",
        a.UPPER_LIMIT "Upper Limit",
        a.DAT_COL_CERT "Required Cert",
        a.OPTIONAL_FLAG "Optional?",
        a.UCF_OPER_DC_FLAG1 "FAI",
        a.UCF_OPER_DC_NUM1 "Seq",
        a.UCF_OPER_DC_NUM2 "Ref",
        a.UCF_OPER_DC_VCH3 "Zone",
        a.UCF_OPER_DC_VCH1 "KPC ID",
        a.UCF_OPER_DC_VCH2 "KPC Type",
        a.UCF_OPER_DC_FLAG2 "RD",
        a.ALT_COUNT "Alterations",
        b.DCVALUE "Data Value", 
        b.COMMENTS "Comments"

    from 
        sfmfg.sfwid_oper_dat_col a left join
        sfmfg.sfwid_serial_oper_dat_col b
        on a.ORDER_ID = b.ORDER_ID
    where a.order_id='{ORDER_ID}' order by OPER_KEY, STEP_KEY, REF_ID, DISPLAY_LINE_NO;
    ''', 'buyoffQuery': f'''
    select 
        a.ORDER_ID "Order Id",
        a.OPER_KEY "Oper Key",
        a.OPER_NO "Oper No",
        a.STEP_KEY "Step Key",
        a.BUYOFF_ID "Buyoff Id",
        a.BUYOFF_TYPE "Buyoff Type",
        a.BUYOFF_CERT "Buyoff Cert",
        a.ALT_COUNT "Alterations",
        a.CROSS_ORDER_FLAG "Cross Order?",
        a.OPTIONAL_FLAG "Optional?"
        
        --a.UPDT_USERID "Updt Userid",
        --a.TIME_STAMP "Time Stamp",
        --a.LAST_ACTION "Last Action",
        --a.STEP_NO "Step No",
        --a.ALT_ID "Alt Id",
        --a.BLOCK_ID "Block Id",
        --a.REF_ID "Ref Id",
        --a.SUSPECT_FLAG "Suspect Flag",
        --a.SLIDE_EMBEDDED_REF_ID "Slide Embedded Ref Id",
        --a.SLIDE_ID "Slide Id",

    from 
        sfmfg.sfwid_oper_buyoff a
    where order_id='{ORDER_ID}' 
    --order by oper_key, step_key, ref_id;
    ''', 'boSerialQuery': f'''
    select 
        a.ORDER_ID "Order Id",
        a.OPER_KEY "Oper Key",
        a.STEP_KEY "Step Key",
        a.BUYOFF_ID "Buyoff Id",
        a.LOT_ID "Lot Id",
        a.SERIAL_ID "Serial Id",
        a.BUYOFF_STATUS "Buyoff Status",
        a.COMMENTS "Comments",
        a.PERCENT_COMPLETE "Percent Complete",
        a.QTY_COMPLETE "Qty Complete",
        a.LAST_ACTION "Last Action",
        a.UPDT_USERID "Updt Userid",
        a.TIME_STAMP "Time Stamp"

        --a.OPER_ITERATION "Oper Iteration",
        --a.OPER_EXE_COUNT "Oper Exe Count",
        
    from 
        sfmfg.sfwid_serial_oper_buyoff a
    where a.order_id='{ORDER_ID}';
    ''', 'opsQuery': f'''
    SELECT
        a.OPER_NO,
        a.TITLE "Oper Title",
        a.OPER_TYPE "Oper Type",
        a.OPER_OPT_FLAG "Optional?",
        a.OCCUR_RATE "Occurs",
        a.ASGND_WORK_LOC "Work Location",
        a.ASGND_WORK_DEPT "Work Dept",
        a.ASGND_WORK_CENTER "Work Center",
        a.TEST_TYPE "Test Type",
        a.OSP_FLAG "Outside Process(OSP)?",
        a.SUPPLIER_CODE "Supplier Code",
        a.OSP_DAYS "OSP Days",
        a.OSP_COST_PER_UNIT "OSP Cost Per Unit",
        a.UCF_PLAN_OPER_FLAG1 "Time Basis Code",
        a.SEQ_STEPS_FLAG "Execute Steps in Sequence?",
        a.UCF_PLAN_OPER_FLAG2 "Pay Point Code",
        a.UCF_PLAN_OPER_NUM4 "Pct Overlap",
        a.UCF_PLAN_OPER_VCH1 "Type Oper Code",
        a.UCF_PLAN_OPER_VCH5 "Machine Code",
        a.UCF_PLAN_OPER_VCH2 "Machine Rate",
        a.UCF_PLAN_OPER_VCH6 "Prime Op",
        a.UCF_PLAN_OPER_VCH7 "EC Letter",
        a.UCF_PLAN_OPER_NUM1 "Run Time Relief Factor",
        a.UCF_PLAN_OPER_NUM5 "Queue Hours",
        a.UCF_PLAN_OPER_NUM3 "Cycle Qty",
        a.UCF_PLAN_OPER_FLAG3 "Schedule/Cost Code",
        a.UCF_PLAN_OPER_FLAG4 "Source Code",
        a.SCHED_UNITS_PER_RUN "Units/Run",
        a.SCHED_MOVE_HOURS "Move Hours",
        a.SCHED_SETUP_TYPE "Setup Type",
        a.SCHED_ENG_STD_FLAG "Eng Std?",
        a.ACTUAL_CREW_QTY "Crew Qty",
        a.ACTUAL_CREW_QTY_SETUP "Crew Qty Setup",
        a.SCHED_MACHINE_HOURS_PER_UNIT "Mach Hrs/Unit",
        a.SCHED_MACHINE_HOURS_SETUP "Mach Hrs Setup",
        a.SCHED_LABOR_HOURS_PER_UNIT "Labor Hrs/Unit",
        a.SCHED_LABOR_HOURS_SETUP "Labor Hrs Setup",
        a.SCHED_LABOR_HOURS_INSPECT "Labor Hrs Inspec",
        a.SCHED_DUR_HOURS_PER_UNIT "Duration Hrs/Unit",
        a.SCHED_DUR_HOURS_SETUP "Duration Hrs Setup",
        a.SCHED_DUR_HOURS_INSPECT "Duration Hrs Inspect",
        a.UCF_PLAN_OPER_VCH8 "Batch Qty",
        a.OPER_STATUS "Oper Status",
        a.ACTUAL_START_DATE "Actual Start Date",
        a.ACTUAL_END_DATE "Actual End Date",
        a.ALT_COUNT "Alterations"
    FROM
        sfmfg.SFWID_OPER_DESC a 
    WHERE 
        ORDER_ID='{ORDER_ID}' order by OPER_NO, EXE_ORDER, NVL(STEP_NO,0);
    '''})

    # write_path = os.path.join(data_dir, f"{order_no}.json")
    # with open(write_path, 'w') as f:
    #     json.dump(query_dict, f, indent=4)
    # return write_path
    return query_dict
    # end create_queries    

def write_dataset(query_dict):
    if not query_dict: return None
    data_tables = OrderedDict()
    for key, val in query_dict.items():
        if key in ['dcQuery and dcSerialQuery combined',]: continue
        query = val.replace(";", "").replace('from', 'FROM').replace('select', 'SELECT', 1)
        new_query = []
        for line in query.split("\n"):
            val = None if line.strip().startswith('--') or line.isspace() else line
            if val:
                new_query.append(val)
        new_query = "\n".join(new_query)
        # print(query_dict); continue
        # print(key)
        try:
            df = get_dataframe(new_query)[0]
        except Exception as e:
            print(key, e)
        # print(len(df)); continue
        cols = columns(new_query)
        data_dict = OrderedDict()
        try:
            if len(cols) > 1:
                for k, v in zip(cols, df):
                    data_dict.update({k:v})
            else:
                data_dict = {cols[0]:df}
        except Exception as e:
            print(key, e)
        datetime_dict = dict()
        for k, v in data_dict.items():
            v = v.isoformat() if isinstance(v, (datetime, date)) else v
            datetime_dict.update({k:v})
        data_dict.update(**datetime_dict)
        data_tables.update({key:data_dict})

    data_tables['Buyoffs'] = OrderedDict({**data_tables.pop('buyoffQuery'), **data_tables.pop('boSerialQuery')})
    data_tables['Data Collections'] = OrderedDict({**data_tables.pop('dcQuery'), **data_tables.pop('dcSerialQuery')})

    # data_path = os.path.join(data_dir, 'WO_data.json')
    # with open(data_path, 'w') as f:
    #     try:
    #         json.dump(data_tables, f)
    #     except Exception as e:
    #         print(e)
    # return data_path
    return data_tables

def create_xml_document(data_dict, file_name):
    # data_path=os.path.join(data_dir, f'{order_no}.json')
    # data_path=data_path
    # with open(data_path, 'r') as json_file:
    #     data_dict = OrderedDict(json.load(json_file))

    data_dict = data_dict

    environment = Environment(loader=FileSystemLoader(templates_dir), trim_blocks=True)
    table_template = environment.get_template("table.xml")
    text_template = environment.get_template("text.xml")
    WO_cont = environment.get_template("workOrderCont.xml")
    WO_head = environment.get_template("WOHdrQuery.xml")

    cont_jsn = dict(dct={**data_dict['WOHdrQuery'], **data_dict['opsQuery']},
                    datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cont_xml = WO_cont.render(cont_jsn).strip()

    doc_dict = OrderedDict()
    doc_dict['Header'] = WO_head.render(cont_jsn)
    for key in data_dict.keys():
        jsn = dict(dct={k: v for k, v in data_dict[key].items() if v})
        jsn.update(**{"title": key})
        if 'text' in key.lower():
            jsn = dict(dct={k: v.replace("<", "&lt;").replace(">", "&gt;")
                    for k, v in jsn.get('dct').items() if isinstance(v, str)})
            tmp_doc = text_template.render(jsn)
        elif key in ['Tools','Buyoffs', 'Data Collections']:
            tmp_doc = table_template.render(jsn)
        else:
            continue
        tmp_doc = environment.from_string('{% extends "masterreference_only.xml" %}\n{% block body %}\n' + cont_xml + tmp_doc + '\n{% endblock %}\n')
        doc_dict[key] = tmp_doc.render(dict(datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    doc_list = []
    while doc_dict:
        _, doc = doc_dict.popitem(last=False)
        doc_list.append(doc)

    doc = "".join(doc_list)
    doc = '{% extends "root.xml" %}\n{% block masterreference_only %}\n' + doc + '\n{% endblock %}\n'
    doc = environment.from_string(doc).render()
    
    # file_name = os.path.split(data_path)
    # name, = os.path.splitext(file_name)
    name = file_name
    docs_path = os.path.join(docs_dir, f'{name}.xml')
    with open(docs_path, 'w') as f:
        f.write(doc)
    return docs_path

###############################################################################

@task()
def get_order_no(N=0):
    data_path = os.path.join(data_dir, 'order_no.json')
    order_no_query = '''
    SELECT DISTINCT ORDER_NO
    FROM sfmfg.sfwid_order_desc
    '''
    try:
        order_no_df = get_dataframe(order_no_query)
        order_no_df = [val[0] for val in order_no_df]
        order_no_df = pd.Series(order_no_df)
    except Exception as e:
        raise (e)
    # return {order_no_df.to_list()}
    order_no_df.to_json(data_path, orient='records', lines=False)
    order_no_list = order_no_df.to_list()[:N] if N > 0 else order_no_df.to_list()

    data_path = os.path.join(data_dir, 'order_no.json')
    with open(data_path, 'w') as json_file:
        json.dump(order_no_list, json_file)
    return None

@task()
def create_queries_task():
    # ons = json.loads(ons)
    data_path = os.path.join(data_dir, 'order_no.json')
    with open(data_path, 'r') as f:
        ons = json.load(f)
    query_path_list = []
    while ons:
        order_no = ons.pop()
        query_path_list.append(create_queries(order_no))
    data_path = os.path.join(data_dir, 'query_table.json')
    with open(data_path, 'w') as json_file:
        json.dump(query_path_list, json_file)
    return None

# create_queries_task()

@task()
def write_dataset_task():
    data_path = os.path.join(data_dir, 'query_table.json')
    with open(data_path, 'r') as json_file:
        qpl = json.load(json_file)
    data_path_list = []
    while qpl:
        query_path = qpl.pop()
        # print(type(query_path)); continue
        data_path = write_dataset(query_path)
        data_path_list.append(data_path)
    data_path = os.path.join(data_dir, 'WO_data.json')
    with open(data_path, 'w') as json_file:
        json.dump(data_path_list, json_file)
    return None

# write_dataset_task()

@task()
def create_xml_document_task():
    data_path = os.path.join(data_dir, 'WO_data.json')
    with open(data_path, 'r') as json_file:
        data_dict_list = json.load(json_file)
    xml_path_list = []
    while data_dict_list:
        data_dict = data_dict_list.pop()
        if not data_dict: continue
        file_name = data_dict.get('WOHdrQuery').get('Order No')
        tmp_docs_path = create_xml_document(data_dict, file_name)
        xml_path_list.append(tmp_docs_path)
    docs_path = os.path.join(docs_dir, 'xml_path_list.json')
    with open(docs_path, 'w') as json_file:
        json.dump(xml_path_list, json_file)
    return None

# create_xml_document_task()

@task()
def print_list() -> None:
    path_list = os.path.join(docs_dir, 'xml_path_list.json')
    with open(path_list, 'r') as f:
        xml_path_list = json.load(f)
    for path in xml_path_list:
        print(path)

# print_list()


default_args = {
    'owner': 'earl',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

@dag(default_args=default_args, schedule_interval='@daily', catchup=False)
def earls_data_generation_dag_v3():
    """
    Dag to illustrate the simple pipeline for processing data to create XSLT-FO
    xml documents based on jinja templates.  
    """
    get_order_no(N=100) >> create_queries_task() >> write_dataset_task() >> create_xml_document_task() >> print_list()

earls_data_generation_dag_v3()
```
