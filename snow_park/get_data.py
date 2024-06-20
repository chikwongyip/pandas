# coding: utf-8
from create_session import create_session
import re
import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import math


def check_type(desc):
    if re.match("MT", desc):
        return "MT"
    elif desc == "特殊渠道" or desc == "OTHERS":
        return "Others"
    else:
        return "GT"


def get_customers():
    session = create_session()
    df_customer = session.sql(
        """
                              select
                              distinct
                              sale.customer,
                              person.description,
                              sale.gc_channel
                              from dim.dim.dim1_t_customer_sales as sale
                              inner join dim.text.text_t_person as person
                              on sale.area_manager_code = person.person
                              where sale.sales_organization in ('HH31','HH41')
                              """
    ).to_pandas()
    session.close()
    df_customer["CUSTOMER_TYPE"] = df_customer["DESCRIPTION"].apply(check_type)
    return df_customer


def get_payment_term():
    session = create_session()
    df_payment = session.table("ods.fico.ods_t_payment_term").to_pandas()
    session.close()
    return df_payment


def get_pod():
    session = create_session()
    df_pod = session.sql(
        """
        select distinct vbeln,
                        podat
                    from darliesf1.sapabap1.t9pod
                    where erdat >= '20230101'
        """
    ).to_pandas()
    session.close()
    return df_pod


def convert_date(date_str):
    if date_str == "00000000" or pd.isna(float(date_str)):
        return None
    else:
        # print(date_str)
        date_str_format = date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[6:8]
        return datetime.strptime(date_str_format, "%Y-%m-%d").date()


def get_ar_open_document(date):
    session = create_session()
    df_1 = session.sql(
        """
        select * from ods.fico.ods_v_ar_document
        where comp_code in ('HH41','HH31')
        and fi_docstat = 'O'
        and to_date(
           concat(
                    substr(pstng_date, 0, 4),
                    '-',
                    substr(pstng_date, 5, 2),
                    '-',
                    substr(pstng_date, 7, 2)
                )
        ) <=  to_date('{p_date}')
        """.format(
            p_date=date
        )
    ).to_pandas()

    df_2 = session.sql(
        """
            select * from ods.fico.ods_v_ar_document
            where comp_code in ('HH41','HH31')
            and to_date(
                concat(
                    substr(pstng_date, 0, 4),
                    '-',
                    substr(pstng_date, 5, 2),
                    '-',
                    substr(pstng_date, 7, 2)
                )
            ) <= '{p_date}'
            and to_date(
                concat(
                    substr(clear_date, 0, 4),
                    '-',
                    substr(clear_date, 5, 2),
                    '-',
                    substr(clear_date, 7, 2)
                )
            ) > '{p_date}'
            and fi_docstat = 'C'
        """.format(
            p_date=date
        )
    ).to_pandas()
    session.close()
    result = pd.concat([df_1, df_2])
    result["DEBITOR"] = result["DEBITOR"].apply(lambda x: x.lstrip("0"))
    return result


def calculate_payment_day(row):
    # print(row)
    if row.AC_DOC_TYP == "KD":
        return (
            row.POSTING_DATE
            if row.POSTING_DATE > row.BASELINE_DATE
            else row.BASELINE_DATE
        )
    if (row.AC_DOC_TYP == "DA" or row.AC_DOC_TYP == "RV") and (
        re.match("4800", row.ALLOC_NMBR) or re.match("3800", row.ALLOC_NMBR)
    ):
        if re.match("G", row.PMNTTRMS):
            if row.POD_DATE is None:
                return None
            else:
                payment_day = row.POD_DATE + timedelta(days=row.DAYS)
                return payment_day
        else:
            if row.LAST_POD_DATE is None:
                return None
            else:
                payment_day = row.LAST_POD_DATE + timedelta(days=row.DAYS)
                
                return payment_day


if __name__ == "__main__":
    df_open_docs = get_ar_open_document("2024-04-30")
    df_customers = get_customers()
    df_pod = get_pod()
    df_payment_term = get_payment_term()
    result = df_open_docs.merge(
        df_customers,
        left_on="DEBITOR",
        right_on="CUSTOMER",
        how="inner",
        suffixes=(False, False),
    )
    result = result.merge(
        df_pod,
        left_on="ALLOC_NMBR",
        right_on="VBELN",
        how="left",
        suffixes=(False, False),
    )
    result = result.merge(
        df_payment_term,
        left_on="PMNTTRMS",
        right_on="ZTERM",
        how="left",
        suffixes=(False, False),
    )
    result["GJAHR"] = "2024-04-30"[:4]
    result["POSTING_DATE"] = pd.to_datetime(result["PSTNG_DATE"].apply(convert_date),format="%Y%m%d")
    result["BASELINE_DATE"] = pd.to_datetime(result["BLINE_DATE"].apply(convert_date),format='%Y%m%d')
    result["POD_DATE"] = pd.to_datetime(result["PODAT"].apply(convert_date),format='%Y%m%d')
    result["LAST_POD_DATE"] = pd.to_datetime(
        result["POD_DATE"], format="%Y%m"
    ) + MonthEnd(1)
    result["DAYS"] = result["DAYS"].apply(lambda x: 0 if math.isnan(x) else int(x))
    result = result.filter(
        items=[
            "COMP_CODE",
            "DEBITOR",
            "FISCPER",
            "GJAHR",
            "CUSTOMER_TYPE",
            "GC_CHANNEL",
            "AC_DOC_NO",
            "ITEM_NUM",
            "DEB_CRE_LC",
            "POSTING_DATE",
            "BASELINE_DATE",
            "POD_DATE",
            "LAST_POD_DATE",
            "REF_DOC_NO",
            "PMNTTRMS",
            "ALLOC_NMBR",
            "AC_DOC_TYP",
            "DAYS",
        ]
    )
    kd = result.query('AC_DOC_TYP == "KD"')
    kd["PAYMENT_DATE"] = kd.apply(calculate_payment_day, axis=1)
    da_rv = result.query('AC_DOC_TYP == "DA" or AC_DOC_TYP == "RV"')
    da_rv["PAYMENT_DATE"] = da_rv.apply(calculate_payment_day, axis=1)
    
    dr_group = da_rv.filter(items=["REF_DOC_NO", "PAYMENT_DATE"])
    
    dr = result.query('AC_DOC_TYP == "DR"')
    dr["PAYMENT_DATE"] = dr.apply(calculate_payment_day, axis=1)