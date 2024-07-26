import os
import fire
import pandas as pd

from data_scraper.nft_data_scraper import NFTDataScraper
from data_preprocessing.rarity_score_calculator import NftRarityRanker
from data_scraper.utils import save_json

from utils.auto_download import stats_upload_and_clear_one_collection_images


def scrap_one_collections(i, collection_contract_address, main_save_dir, chain, activity_types, if_scrap_items=True, if_download_images=False, max_downloaded_images='inf', if_reuse_activities=True):
    scraper = NFTDataScraper(main_save_dir=main_save_dir, num_threads=20, max_retries=0, max_timeout=120, max_connect_timeout=60)
    existence_flag, success_flag, error_info = False, False, ''
    print('collection_id', i)
    for n in range(3):
        print(f'Retry {n-1} times for collection_id {i}') if n > 0 else None
        # try:
        scraper.scrape_collection(
            collection_contract_address=collection_contract_address, 
            chain=chain, 
            activity_types=activity_types,
            if_reuse_activities=if_reuse_activities,
            if_scrap_items=if_scrap_items,
            if_download_images=if_download_images,
            max_downloaded_images=max_downloaded_images)
        success_flag = True
        break
        # except Exception as e:
        #     error_info = e
        #     print(e)
    existence_flag = os.path.exists(os.path.join(main_save_dir, chain, collection_contract_address, 'information.json'))
    print()
    if not existence_flag: success_flag = False
    return existence_flag, success_flag, error_info


def scrap_top_collections_data(from_id=0, to_id=None, if_scrap_items=False, if_download_images=False, max_downloaded_images='inf', if_reuse_activities=True):
    # basic settings
    main_save_dir = './data/nft_data'
    chain = 'ETHEREUM'
    activity_types = ['BID', 'List']  # 'MINT', 'List', 'BID'
    # collection list
    pd_top_collections_info = pd.read_json('./shared_data/top_collections_info-2023-02-12T15:54:43.json')
    if to_id is None or to_id == 'None':
        to_id = len(pd_top_collections_info)
    from_id, to_id = int(from_id), int(to_id)
    pd_top_collections_info = pd_top_collections_info[from_id:to_id]
    # start to scrape the data
    collection_scrap_status_list = []
    log_fpath = f'./logs/collection_scrap_status_{from_id}_{to_id}.csv'
        
    for i, collection_contract_address in zip(pd_top_collections_info.index, pd_top_collections_info['address']): # collection_contract_address
    # for i, collection_contract_address in zip([1], ['0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d']): # collection_contract_address
        if collection_contract_address is None:
            continue
        print(f'collection_id {i:4d}: {collection_contract_address}')
        collection_fdir = os.path.join(main_save_dir, chain, collection_contract_address)
        existence_flag, success_flag, error_info = scrap_one_collections(
            i, collection_contract_address, main_save_dir, chain, activity_types, 
            if_download_images=if_download_images, max_downloaded_images=max_downloaded_images,
            if_reuse_activities=if_reuse_activities, if_scrap_items=if_scrap_items)
        collection_scrap_status_list.append(
            {'id': i, 
             'address': collection_contract_address, 
             'existence_flag': existence_flag, 
             'success_flag': success_flag,
             'error_info': error_info})
        pd.DataFrame(collection_scrap_status_list).to_csv(log_fpath)
        # stats_upload_and_clear_one_collection_images(collection_contract_address, collection_fdir, if_upload=True, if_count=False, if_delete=True)
        # stats_upload_and_clear_one_collection_images(collection_contract_address, collection_fdir, if_upload=True, if_count=False, if_delete=False)
    print(f'Save log to {log_fpath}')


if __name__=="__main__":
    fire.Fire(scrap_top_collections_data)