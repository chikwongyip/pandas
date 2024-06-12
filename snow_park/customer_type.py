# coding: utf-8
from create_session import create_session
import re


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
