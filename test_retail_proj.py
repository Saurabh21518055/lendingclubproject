from lib.Utils import get_spark_session
import pytest
from lib.DataReader import read_customers
from lib.DataReader import read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state,filter_orders_generic
from lib.ConfigReader import get_app_config

# @pytest.mark.slow()
@pytest.mark.skip('work in progress')

def test_read_customers_df(spark):
    customers_count=read_customers(spark,'LOCAL').count()
    assert customers_count==12435

# @pytest.mark.transformation() # this is an user defined markers   
@pytest.mark.skip('work in progress')
 
def test_read_orders_df(spark):
    orders_count=read_orders(spark,'LOCAL').count()
    assert orders_count==68884

# @pytest.mark.transformation() 
@pytest.mark.skip('work in progress')
   
def test_filter_closed_orders(spark):
    orders_df=read_orders(spark,'LOCAL')
    filtered_count= filter_closed_orders(orders_df).count()
    assert filtered_count==7556

# @pytest.mark.
@pytest.mark.skip('work in progress')
def test_read_get_app_config():
   config= get_app_config('LOCAL')
   assert config['orders.file.path']=='data/orders.csv'

@pytest.mark.skip('work in progress')
def test_count_orders_state(spark,expected_results):
   customers_df= read_customers(spark,'LOCAL')
   actual_results=count_orders_state(customers_df)
   assert actual_results.collect()==expected_results.collect()
   #    assert sorted(actual_results.collect()) == sorted(expected_results.collect()), "Mismatch in results"

@pytest.mark.skip()
def test_check_closed_count(spark):
    orders_df=read_orders(spark,'LOCAL')
    filter_count=filter_orders_generic(orders_df,'CLOSED').count()
    assert filter_count==7556

# @pytest.mark.latest()
@pytest.mark.skip()
def test_check_complete_count(spark):
    orders_df=read_orders(spark,'LOCAL')
    filter_count=filter_orders_generic(orders_df,'COMPLETE').count()
    assert filter_count==22900

@pytest.mark.parametrize(
        "status,count",
        [('CLOSED',7556),
        ('COMPLETE',22900),
         ('PENDING_PAYMENT',15030)
        ]
)
def test_check_count(spark,status,count):
        orders_df=read_orders(spark,'LOCAL')
        filter_count=filter_orders_generic(orders_df,status).count()
        assert filter_count==count

