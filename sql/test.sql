select * from ods.fico.ODS_V_AR_DOCUMENT limit 100;

select
                              distinct
                              sale.customer,
                              person.description,
                              sale.gc_channel
                              from dim.dim.dim1_t_customer_sales as sale
                              inner join dim.text.text_t_person as person
                              on sale.area_manager_code = person.person;
select * from dim.dim.dim1_t_customer_sales limit 100;
 select *
                    from darliesf1.sapabap1.t9pod where erdat >= '20230101' limit 100;


select * from dwd.ecom.dwd_t_ecom_order_getlist_items limit 100;
select * from dwd.ecom.dwd_t_ecom_order_getlist limit 100;

select * from dwd.ecom.dwd_t_ecom_order_getlist_objects limit 100;

select * from dwd.ecom.dwd_t_ecom_order_getlist_pmts limit 100;
