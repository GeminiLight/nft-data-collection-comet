import os
import copy
import json
import time
import tqdm
import httpx
import asyncio
import threading
import pprint
import pandas as pd
from datetime import datetime


class NftAssetQuerier:

    def __init__(
            self, 
            api_key,
            main_save_dir='save', 
            num_threads=50, 
            max_timeout=120., 
            sleep_time=0.1, 
            page_size=1000,
            image_sub_save_dir='images',
            if_save=True,
        ) -> None:
        self.alchemy_base_url = f'https://eth-mainnet.g.alchemy.com/nft/v2/{api_key}/getNFTs/'
        self.main_save_dir = main_save_dir
        self.image_sub_save_dir = image_sub_save_dir
        self.sleep_time = sleep_time
        self.page_size = page_size
        self.num_threads = num_threads
        self.if_save = if_save
        timeout = httpx.Timeout(max_timeout, connect=60.0)
        transport = httpx.HTTPTransport(retries=3)
        self.client = httpx.Client(timeout=timeout, transport=transport, follow_redirects=True)
        assert self.page_size <= 1000

    def ready(self, collection_contract_address, chain='ETHEREUM'):
        collection_contract_address = collection_contract_address.lower()
        chain = chain.upper()
        self.collection_save_dir = os.path.join(self.main_save_dir, chain, collection_contract_address)
        collection_image_dir = os.path.join(self.collection_save_dir, self.image_sub_save_dir)
        if not os.path.exists(collection_image_dir):
            os.makedirs(collection_image_dir)
        return chain, collection_contract_address

    def safe_request(self, url, method='get', **kwargs):
        res = self.client.request(method, url, **kwargs)
        res_status = res.status_code
        if res_status == 200:
            res_data = res.json()
        elif res_status == 404:
            print(f'Status of Request is {res_status}: {res}')
            print(res.json())
            res_data = None
        else:
            print(f'Status of Request is {res_status}: {res}')
            print(res.json())
            res_data = None
            raise ConnectionError
        return res_status, res_data

    def scrape_owner_asset(self, owner_addr):
        url = f'{self.alchemy_base_url}?owner={owner_addr}'
        res_status, res_data = self.safe_request(url, method='get')
        if res_data is None:
            return None
        save_json(res_data, 'owner_addr.json')
        pprint.pprint(res_data)
        return res_data
        # all_collections_file_path = f'{os.path.join(self.main_save_dir)}/all_collections.json'
        # print(f'Save all_collections file to {all_collections_file_path}')
        

def save_json(json_obj, file_path):
    with open(file_path, 'w') as f:
        f.write(json.dumps(json_obj, indent=4))
    return file_path


def read_json(file_path):
    with open(file_path, 'r') as f:
        json_file = json.load(f)
    return json_file


if __name__=='__main__':
    import os
    api_key = os.getenv('ALCHEMY_API_KEY')
    nft_asset_querier = NftAssetQuerier(api_key, if_save=False)
    nft_asset_querier.scrape_owner_asset('0x745698C2125Dd6f5464038F7C77977f33eAfBc24')