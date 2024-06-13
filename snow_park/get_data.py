# coding: utf-8
from create_session import create_session
import re
import pandas as pd


def check_type(desc):
    if re.match("MT", desc):
        return "MT"
    elif desc == "特殊渠道" or desc == "OTHERS":
        return "Others"
    else:
        return "GT"


def get_customer_type():
    session = create_session()
    df_customer = session.sql(
        """
                              select sale.customer,
                              person.description,
                              sale.gc_channel
                              from dim.dim.dim1_t_customer_sales as sale
                              inner join dim.text.text_t_person as person
                              on sale.area_manager_code = person.person
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


def get_pod_date():
    session = create_session()
    df_pod = session.sql(
        """
        select distinct vbeln,
                        podat
                    from darliesf1.sapabap1.t9pod
        """
    ).to_pandas()
    session.close()
    return df_pod


def get_ar_document(date):
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
            p_date = date
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
            p_date = date
        )
    ).to_pandas()
    session.close()
    result = pd.concat([df_1, df_2])
    return result

if __name__ == "__main__":
    date = "2024-04-30"
    get_ar_document(date)
    # print(result)