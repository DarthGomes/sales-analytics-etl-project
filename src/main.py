from src.utils.logger import get_logger

from src.ingestion import run_ingestion

from src.transformations.store_products_transformation import run_store_products
from src.transformations.store_sales_order_detail import run_store_sales_order_detail
from src.transformations.store_sales_order_header import run_store_sales_order_header

from src.transformations.publish_products import run_publish_products
from src.transformations.publish_products_propose import run_publish_products_propose
from src.transformations.publish_orders import run_publish_orders

logger = get_logger(__name__)


def main():

    logger.info("Starting Sales Analytics Pipeline")

    try:

        # RAW
        run_ingestion()

        # STORE
        run_store_products()
        run_store_sales_order_detail()
        run_store_sales_order_header()

        # PUBLISH
        run_publish_products()
        run_publish_orders()
        run_publish_products_propose()

        logger.info("Pipeline finished successfully")

    except Exception as e:

        logger.exception("Pipeline failed")
        raise e


if __name__ == "__main__":
    main()