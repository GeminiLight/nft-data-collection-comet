import os
import time
import httpx
import pandas as pd
import concurrent
from pprint import pprint


class NftElementScraper:
    """
    Scrap data from Element Market API

    Limit: 120 requests per minute
    """
    def __init__(
            self, 
            save_dir,
            api_key, 
            chain='ethereum', 
            timeout=120, 
            max_retries=2, 
            if_save=True,
            if_reuse=True,
            sleep_time=1,
            verbose=True
        ):
        self.save_dir = save_dir
        self.chain_dir = os.path.join(self.save_dir, chain)
        self.api_key = api_key
        self.base_url = 'https://api.element.market/openapi/v1'
        headers = {
            "accept": "application/json",
            "X-Api-Key": self.api_key
        }
        transport = httpx.HTTPTransport(retries=max_retries)
        self.client = httpx.Client(headers=headers, timeout=timeout, transport=transport)
        self.if_save = if_save
        self.if_reuse = if_reuse
        self.verbose = verbose
        self.sleep_time = sleep_time
        if if_save:
            self._create_dir()

    def _create_dir(self):
        if not os.path.exists(self.save_dir):
            os.mkdir(self.save_dir)
        if not os.path.exists(self.chain_dir):
            os.mkdir(self.chain_dir)

    def _ready_for_collection(self, collection_address: str):
        collection_address = collection_address.lower()
        self.collection_dir = os.path.join(self.chain_dir, collection_address)
        if not os.path.exists(self.collection_dir):
            os.mkdir(self.collection_dir)
        return collection_address

    def _request(self, url, method='get', **kwargs):
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

    def scrap_collection_info(self, collection_address: str = None, collection_slug: str = None):
        if collection_address:
            url = f'{self.base_url}/contract?contract_address={collection_address}'
        elif collection_slug:
            url = f'{self.base_url}/contract?contract_slug={collection_slug}'
        res_status, res_data = self._request(url)
        res_data = res_data['data']
        return res_data
    
    def scrap_collection_stats(self, collection_address: str):
        url = f'{self.base_url}/contract/stats?contract_address={collection_address}'
        res_status, res_data = self._request(url)
        res_data = res_data['data']
        return res_data

    def scrap_collection_activities(self, collection_address: str, event_type: str = 'Offer', limit: int = 50):
        collection_address = self._ready_for_collection(collection_address)
        event_type_name = ''.join([word.capitalize() for word in  event_type.split('_')])
        assert event_type_name in ['Sale', 'Minted', 'List', 'Offer', 'Transfer', 'CollectionOffer', 'BidCancel', 'Cancel']
        activities_file_path = os.path.join(self.collection_dir, f'activities_{event_type.lower()}.csv')
        if self.if_reuse and os.path.exists(activities_file_path):
            df_downloaded_activities = pd.read_csv(activities_file_path)
            latest_cursor = df_downloaded_activities['cursor'].iloc[0]
            downloaded_oldest_time = df_downloaded_activities['event_time'].min()
            downloaded_latest_time = df_downloaded_activities['event_time'].max()
            print(f'Reuse {len(df_downloaded_activities)} {event_type} Activities with the range of {downloaded_oldest_time} to {downloaded_latest_time}') if self.verbose else None
        else:
            df_downloaded_activities = None
            latest_cursor = None
        url = f'{self.base_url}/assetEvents?chain=eth&contract_address={collection_address}&limit={limit}&event_names={event_type_name}'
        all_activities_list = []
        request_count = 0
        while True:
            res_status, res_data = self._request(url)
            res_data = res_data['data']
            page_info = res_data.get('pageInfo', None)
            if page_info is None or page_info['hasNextPage'] == 'false': break
            activities = res_data['assetEventList']
            if request_count != 0 and request_count % 40 == 0:
                print(f'Requested {request_count} times and downloaded {len(all_activities_list)} {event_type} activities for {collection_address}') if self.verbose else None
            request_count += 1
            if len(activities) == 0:
                break
            current_cursor_list = [activity['cursor'] for activity in activities]
            if latest_cursor in current_cursor_list:
                newly_activities_list = activities[:current_cursor_list.index(latest_cursor)]
                all_activities_list += newly_activities_list
                break
            all_activities_list += activities
            if res_data['pageInfo']['hasNextPage']:
                cursor = res_data["pageInfo"]["endCursor"]
                url = f'{self.base_url}/assetEvents?chain=eth&contract_address={collection_address}&limit={limit}&event_names={event_type_name}&cursor={cursor}'
            else:
                break
            time.sleep(self.sleep_time)
        activities_key_info_list = extract_key_info_from_list_event(collection_address, all_activities_list)
        if len(activities_key_info_list) == 0 and df_downloaded_activities is None:
            print(f'No {event_type} activities for {collection_address}') if self.verbose else None
            return None
        df_activities = pd.DataFrame(activities_key_info_list)
        num_newly_activities = len(df_activities)
        if df_downloaded_activities is not None:
            df_activities = pd.concat([df_downloaded_activities, df_activities], axis=0)
        oldest_time = df_activities['event_time'].min()
        latest_time = df_activities['event_time'].max()
        print(f'Downloaded {len(df_activities)} (new {num_newly_activities}) {event_type} activities with range from {num_newly_activities} newly activities with the range of {oldest_time} to {latest_time}') if self.verbose else None
        df_activities['event_time'] = pd.to_datetime(df_activities['event_time'])
        df_activities = df_activities.sort_values(by='event_time', ascending=False)
        if self.if_save:
            df_activities.to_csv(activities_file_path, index=False) if self.verbose else None
            print(f'Saved {event_type} Activities to {activities_file_path}') if self.verbose else None
        return df_activities

def extract_key_info_from_list_event(collection_address, activities: list):
    key_info_list = []
    for i, activity in enumerate(activities):
        activity_cursor = activity['cursor']
        activity_info = activity['assetEvent']
        key_info = {}
        key_info['tx_hash'] = activity_info['txHash']
        key_info['event_time'] = activity_info['eventTime']
        key_info['expire_time'] = activity_info['expireTime']
        key_info['collection_address'] = collection_address
        key_info['event_type'] = activity_info['eventName'].lower()
        key_info['token_id'] = activity_info['tokenId']
        key_info['from_address'] = activity_info['fromAddress']
        key_info['to_address'] = activity_info['toAddress']
        key_info['quantity'] = activity_info['quantity']
        key_info['payment_token'] = activity_info['paymentToken']
        key_info['price'] = activity_info['price']
        key_info['tx_to_address'] = activity_info['txToAddress']
        key_info['orign_event_address'] = activity_info['orignEventAddress']
        key_info['cursor'] = activity_cursor
        key_info_list.append(key_info)
    return key_info_list


def scrap_collection_multiple_activities(collection_address, event_types):
    print(f'Scraping {event_types} activities for {collection_address}')
    for event_type in event_types:
        element_scraper.scrap_collection_activities(collection_address, event_type=event_type, limit=50)
    print(f'Completely scraped {event_type} activities for {collection_address}')


def scrap_all_collection_multiple_activities(collection_address_list, event_types, max_workers=2):
    
    mt_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    results = []
    for collection_address in collection_address_list:
        res = mt_pool.submit(scrap_collection_multiple_activities, collection_address, event_types)
        results.append(res)
    count = 0
    for res in concurrent.futures.as_completed(results):
        print(res.result())
        count += 1
        print(f'Complete {count} collections. ({count}/{len(collection_address_list)})')

def get_media_account_info(client, contract_address):
    def none_to_string(username):
        return username if username is not None else ''

    res_data = client.fetch_contract_information(contract_address)
    if res_data is None: return None
    collection_info = res_data['collection']
    media_account_info = {
        'collection_contract_address': contract_address,
        'media_twitter_username': none_to_string(collection_info['twitterUrl']).replace('https://twitter.com/', ''),
        'media_instagram_username': none_to_string(collection_info['instagramUrl']).replace('/', ''),
        'media_facebook_username': none_to_string(collection_info['facebookUrl']),
        'media_medium_username': none_to_string(collection_info['mediumUrl']),
        'media_telegram_username': none_to_string(collection_info['telegramUrl']),
        'media_discord_username': none_to_string(collection_info['discordUrl']).replace('https://discord.gg/', '').replace('https://discord.gg/invite/', ''),
    }
    return media_account_info


if __name__ == '__main__':
    data_dir = 'data/element'
    api_key = '5c1b19a8545f158ab79f21e3b7af0135'
    element_scraper = NftElementScraper(save_dir=data_dir, api_key=api_key)
    collection_address = '0xfaf8f2c1428adb9f007d652759b2d8da406e54af'
    # element_scraper.scrap_collection_info(collection_address)
    # element_scraper.scrap_collection_stats(collection_address)
    # event_type_list = ['sale', 'minted', 'list', 'offer', 'transfer', 'collectionoffer', 'bidcancel', 'cancel']
    df_collection_info = pd.read_csv('./data/shroomdk/top_10000000_collections_info_2017-01-01_2023-04-01.csv')
    collection_address_list = df_collection_info['address'].tolist()[0:10]
    event_type_list = ['list', 'offer', 'collection_offer', 'bid_cancel', 'cancel']
    scrap_all_collection_multiple_activities(collection_address_list, event_type_list, max_workers=2)