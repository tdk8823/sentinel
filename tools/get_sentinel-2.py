import os
import datetime
from dateutil.relativedelta import relativedelta
import argparse
import tenacity

from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt


@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(1800))
def download_all(*args, **kwargs):
    return api.download_all(*args, **kwargs)

def query_sentinel(start_date, end_date, level, cloud_percentage, aoi_path, dst_dir):

    # get metadata
    footprint = geojson_to_wkt(read_geojson(aoi_path))
    products = api.query(footprint,
                        date = (start_date, end_date), 
                        platformname = 'sentinel-2',
                        processinglevel = level,
                        cloudcoverpercentage = (0,cloud_percentage)
                        )
    # raise error if not product found.
    assert len(products)>0, 'No products found.'

    # sort by cloud percentage and choise best product id
    df_products = api.to_geodataframe(products)
    df_products = df_products.sort_values(['cloudcoverpercentage'], ascending=[True])
    product_id = df_products.uuid.iloc[0]

    downloaded, triggered, failed = download_all([product_id], directory_path=dst_dir)

    return downloaded, triggered, failed


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--aoi_path',
                        help='path to geojson file which defines Area of Intersest',
                        default='../data/aoi/aoi.gejson'
                        )
    parser.add_argument('--dst_dir',
                        help='path to destination directory',
                        default='./'
                        )
    parser.add_argument('--start_date',
                        help='imagery search date range(stard)'
                        )
    parser.add_argument('--end_date',
                        help='imagery search date range(end)'
                        )
    parser.add_argument('-c', '--cloud_percentage',
                        help='maximum percentage of cloud coverage',
                        default=80
                        )
    parser.add_argument('--level',
                        help='product process level',
                        default='Level-1C'
                        )
    parser.add_argument('--monthly',
                        action='store_true',
                        help='if activated, get minimum cloud coverage product every month'
                        )
    return parser.parse_args()
 
if __name__ == '__main__':

    # get user infomation
    DHUS_USER = os.getenv("DHUS_USER")
    DHUS_PASSWORD = os.getenv("DHUS_PASSWORD")
    api = SentinelAPI(DHUS_USER, DHUS_PASSWORD)
 
    # options 
    args = get_args()
    
    if args.monthly:
        # get best product every month
        start_month = datetime.datetime.strptime(args.start_date, "%Y%m%d")
        end_month   = datetime.datetime.strptime(args.end_date, "%Y%m%d")
        start_date = start_month
        downloaded_cnt = 0
        triggered_cnt = 0
        failed_cnt = 0
        while start_date<=end_month:
            end_date = start_date + relativedelta(months=1) - relativedelta(days=1)

            downloaded, triggered, failed = query_sentinel(
                                            start_date.strftime('%Y%m%d'),
                                            end_date.strftime('%Y%m%d'),
                                            args.level,
                                            args.cloud_percentage,
                                            args.aoi_path,
                                            args.dst_dir
                                            )
            if len(downloaded)>0:
                downloaded_cnt += 1
            if len(triggered)>0:
                triggered_cnt += 1
            if len(failed)>0:
                failed_cnt += 1
            
            start_date += relativedelta(months=1)
        
        print(f"downloaded products: {downloaded_cnt}")
        print(f"triggered products: {triggered_cnt}")
        print(f"failed products: {failed_cnt}")

    else:
        # get best product
        downloaded, triggered, failed = query_sentinel(
                                        args.start_date,
                                        args.end_date,
                                        args.level,
                                        args.cloud_percentage,
                                        args.aoi_path,
                                        args.dst_dir
                                        )
        if len(downloaded)>0:
            product_id = list(downloaded.keys())[0]
            print(f"Product {downloaded[product_id]['title']} is downloaded.")
        if len(triggered)>0:
            product_id = list(triggered.keys())[0]
            print(f"Product {triggered[product_id]['title']} is triggered.")
        if len(failed)>0:
            product_id = list(failed.keys())[0]
            print(f"Product {failed[product_id]['title']} is failed.")
