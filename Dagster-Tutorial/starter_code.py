from dagster import asset, AssetExecutionContext, AssetIn
from typing import List


@asset
def my_first_asset(context: AssetExecutionContext):
    """
    This is our first asset for testing purpose.
    """
    print("This is a print message")
    context.log.info("This is a log message")
    return [1, 2, 3]


# @asset(deps=[my_first_asset])
# def my_second_asset(context: AssetExecutionContext):
#     """
#     This is our second asset for testing purpose.
#     """
#     data = [4, 5, 6]
#     context.log.info(f"Output data is {data}")
#     return data


# @asset(deps=[my_first_asset])
# def my_second_asset(context: AssetExecutionContext, my_first_asset: List):
#     """
#     This is our second asset for testing purpose.
#     """
#     data = my_first_asset + [4, 5, 6]
#     context.log.info(f"Output data is {data}")
#     return data


@asset(ins={"upstream": AssetIn(key="my_first_asset")})
def my_second_asset(context: AssetExecutionContext, my_first_asset: List):
    """
    This is our second asset for testing purpose.
    """
    data = my_first_asset + [4, 5, 6]
    context.log.info(f"Output data is {data}")
    return data
