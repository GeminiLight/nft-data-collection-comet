import os
import time
import fire
import datetime
import concurrent
import pandas as pd
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from flipside import Flipside


SQL_TEMPLATE_BLOCK_INFO = """
SELECT
    block_number,
    block_timestamp,
    difficulty,
    gas_limit,
    gas_used,
    nonce,
    size,
    total_difficulty,
    tx_count as transaction_count,
    miner
FROM
    ethereum.core.fact_blocks
WHERE
    block_number >= 'START_BLOCK_NUMBER_PLACEHOLDER'
ORDER BY
    block_number
LIMIT
   LIMIT_PLACEHOLDER
"""

SQL_TEMPLATE_TOP_COLLECTIONS = """
with
    top_collections as (
        SELECT
            nft_address as address,
            count(block_timestamp) as num_sales,
            max(price) as max_price,
            max(price_usd) as max_price_usd,
            min(price) as min_price,
            min(price_usd) as min_price_usd,
            sum(price) as volume,
            sum(price_usd) as volume_usd
        FROM
            ethereum.nft.ez_nft_sales
        WHERE
            block_timestamp BETWEEN 'START_BLOCK_TIMESTAMP_PLACEHOLDER' AND 'END_BLOCK_TIMESTAMP_PLACEHOLDER'
            AND currency_symbol IN ('ETH', 'WETH')
        GROUP BY
            nft_address
        ORDER BY
            volume desc
    )
SELECT
    b.name,
    b.symbol,
    b.address,
    b.decimals,
    b.contract_metadata,
    a.num_sales,
    a.volume,
    a.volume_usd,
    a.max_price,
    a.max_price_usd,
    a.min_price,
    a.min_price_usd
FROM
    top_collections a
    JOIN ethereum.core.dim_contracts b
WHERE
    a.address = b.address
    AND
    a.volume_usd IS NOT NULL
ORDER BY
    volume_usd desc
LIMIT
   LIMIT_PLACEHOLDER
"""

SQL_TEMPLATE_NFT_METADATA = """
SELECT
  *
FROM
  ethereum.core.dim_nft_metadata
WHERE
    CONTRACT_ADDRESS = 'ADDRESS_PLACEHOLDER'
"""

SQL_TEMPLATE_NFT_SALES = """
SELECT
    a.tx_hash,
    a.block_number,
    a.block_timestamp,
    a.project_name,
    a.nft_address,
    a.tokenid,
    a.erc1155_value,
    a.platform_name,
    a.platform_address,
    a.event_type,
    a.buyer_address,
    a.seller_address,
    a.origin_from_address,
    a.currency_symbol,
    a.currency_address,
    a.price,
    a.price_usd,
    a.creator_fee,
    a.creator_fee_usd,
    a.platform_fee,
    a.platform_fee_usd,
    a.tx_fee,
    a.tx_fee_usd,
    a.total_fees,
    a.total_fees_usd
FROM
    ethereum.core.ez_nft_sales a
WHERE
    a.nft_address = 'ADDRESS_PLACEHOLDER'
    AND a.block_timestamp > 'START_BLOCK_TIMESTAMP_PLACEHOLDER'
    AND a.block_timestamp < 'END_BLOCK_TIMESTAMP_PLACEHOLDER'
"""

SQL_TEMPLATE_NFT_TRANSFERS = """
SELECT
  a.tx_hash,
  a.block_number,
  a.block_timestamp,
  a.event_type,
  a.event_index,
  a.nft_from_address,
  a.nft_to_address,
  a.project_name,
  a.nft_address,
  a.tokenid,
  a.erc1155_value
FROM
  ethereum.core.ez_nft_transfers a
WHERE
  a.nft_address = 'ADDRESS_PLACEHOLDER'
  AND a.block_timestamp > 'START_BLOCK_TIMESTAMP_PLACEHOLDER'
  AND a.block_timestamp < 'END_BLOCK_TIMESTAMP_PLACEHOLDER'
"""

SQL_TEMPLATE_NFT_MINTS = """
SELECT
  a.tx_hash,
  a.block_timestamp,
  a.block_number,
  a.event_type,
  a.project_name,
  a.nft_address,
  a.tokenid,
  a.erc1155_value,
  a.nft_from_address,
  a.nft_to_address,
  a.mint_price_eth,
  a.mint_price_usd,
  a.nft_count,
  a.amount,
  a.amount_usd,
  a.mint_price_tokens,
  a.mint_price_tokens_usd,
  a.mint_token_symbol,
  a.mint_token_address,
  a.tx_fee,
  a.ingested_at
FROM
  ethereum.core.ez_nft_mints a
WHERE
  a.nft_address = 'ADDRESS_PLACEHOLDER'
  AND a.block_timestamp > 'START_BLOCK_TIMESTAMP_PLACEHOLDER'
  AND a.block_timestamp < 'END_BLOCK_TIMESTAMP_PLACEHOLDER'
"""

SQL_TEMPLATE_DAILY_VOLUME = """
SELECT
    DATE_TRUNC('day', block_timestamp) AS date,
    nft_address,
    project_name,
    SUM(price_usd) AS total_volume_usd
FROM
    ethereum.core.ez_nft_sales
WHERE
    DATE_TRUNC('day', block_timestamp) BETWEEN 'START_BLOCK_TIMESTAMP_PLACEHOLDER' AND 'END_BLOCK_TIMESTAMP_PLACEHOLDER'
GROUP BY
    date,
    nft_address,
    project_name
ORDER BY
    date,
    nft_address
"""


SQL_TEMPLATE_DICT = {
    'top_collections': SQL_TEMPLATE_TOP_COLLECTIONS,
    'nft_metadata': SQL_TEMPLATE_NFT_METADATA,
    'nft_sales': SQL_TEMPLATE_NFT_SALES,
    'nft_transfers': SQL_TEMPLATE_NFT_TRANSFERS,
    'nft_mints': SQL_TEMPLATE_NFT_MINTS,
    'daily_volume': SQL_TEMPLATE_DAILY_VOLUME
}


class NftShroomDKScraper:
    """
    
    The following cleanings are necessary:
    1. Remove all the NFTs with 0 volume
    
    """
    max_page_size = 100000
    activity_types = ['sale', 'transfer', 'mint']

    def __init__(self, api_key, main_dir, page_size=100000, if_save=True, deadline_timestamp=None) -> None:
        self.sdk = Flipside(api_key, "https://api-v2.flipsidecrypto.xyz")
        self.main_dir = main_dir
        self.chain = 'ethereum'
        self.init_timestamp = '2016-01-01'
        self.page_size = page_size
        self.if_save = if_save
        self.chain_dir = os.path.join(self.main_dir, self.chain)
        self.deadline_timestamp = self.get_current_timestamp() if deadline_timestamp is None else deadline_timestamp
        assert self.page_size <= self.max_page_size, f'Page size must be less than {self.max_page_size}'
        for dir in [self.main_dir, self.chain_dir]:
            if not os.path.exists(dir):
                os.makedirs(dir, exist_ok=True)
        print(f'Deadline timestamp: {self.deadline_timestamp}')

    def get_current_timestamp(self):
        return (datetime.datetime.now()-datetime.timedelta(days=3)).strftime('%Y-%m-%d')

    def ready_for_address(self, address):
        address = address.lower()
        self.collection_dir = os.path.join(self.chain_dir, address)
        if not os.path.exists(self.collection_dir):
            os.makedirs(self.collection_dir, exist_ok=True)
        return address

    def get_page_records(self, sql, page_number=1):
        query_result_set = self.sdk.query(sql, page_size=self.page_size, page_number=page_number)
        if not query_result_set.error is None:
            raise Exception(query_result_set.error)
        if query_result_set.records is None:
            return []
        return query_result_set.records

    def get_all_records(self, sql):
        all_records = []
        page_number = 1
        print(f'Page Number (Page Size): ', end='')
        while True:
            records = self.get_page_records(sql, page_number=page_number)
            print(f'{page_number} ({len(records)}), ', end='')
            if page_number % 10 == 0: print()
            all_records += records
            if len(records) < self.page_size: break
            page_number += 1
        print()
        return all_records

    def scrap_collection_daily_volume(self, start_timestamp=None, end_timestamp=None):
        if start_timestamp is None: start_timestamp = self.init_timestamp
        if end_timestamp is None: end_timestamp = self.deadline_timestamp
        sql = SQL_TEMPLATE_DICT['daily_volume'].replace('START_BLOCK_TIMESTAMP_PLACEHOLDER', start_timestamp).replace('END_BLOCK_TIMESTAMP_PLACEHOLDER', end_timestamp)
        records = self.get_all_records(sql)
        df_daily_volume = pd.DataFrame(records)
        df_daily_volume['date'] = pd.to_datetime(df_daily_volume['date'])
        df_daily_volume['total_volume_usd'] = df_daily_volume['total_volume_usd'].astype(float)
        df_daily_volume = df_daily_volume.sort_values(by='date')
        print(f'Actual start date: {df_daily_volume["date"].min()} ~ end date: {df_daily_volume["date"].max()}')
        if self.if_save:
            file_path = os.path.join(self.main_dir, f'collection_daily_volume_{start_timestamp}_{end_timestamp}.csv')
            df_daily_volume.to_csv(file_path, index=False, escapechar='\\')
            print(f'Saved collection daily volume to {file_path}')
        return df_daily_volume

    def scrap_top_collections(self, start_block_timestamp='2023-03-01', end_block_timestamp=None, limit=None):
        if limit is None: limit = self.max_page_size
        if end_block_timestamp is None: end_block_timestamp = self.deadline_timestamp
        sql_template = SQL_TEMPLATE_DICT['top_collections']
        sql_top_collections = sql_template.replace('START_BLOCK_TIMESTAMP_PLACEHOLDER', start_block_timestamp) \
                                          .replace('END_BLOCK_TIMESTAMP_PLACEHOLDER', end_block_timestamp) \
                                          .replace('LIMIT_PLACEHOLDER', str(limit))
        print(f'Scraping top collections info from {start_block_timestamp} to {end_block_timestamp}')
        records = self.get_all_records(sql_top_collections)
        print(f'Scraped {len(records)} top collections info')
        if len(records) == 0: return None
        df_top_collections = pd.DataFrame(records)
        df_top_collections.columns = df_top_collections.columns.str.lower()
        if self.if_save:
            top_collections_file_path = os.path.join(self.main_dir, f'top_{int(limit)}_collections_info_{start_block_timestamp}_{end_block_timestamp}.csv')
            df_top_collections.to_csv(top_collections_file_path, index=False, escapechar='\\')
            print(f'Saved top collections info to {top_collections_file_path}')
        return df_top_collections
    
    def scrap_block_info(self, max_count=0):
        block_info_file_path = os.path.join(self.main_dir, f'block_info.csv')
        if os.path.exists(block_info_file_path):
            df_reused_block_info = pd.read_csv(block_info_file_path)
            start_block_number = df_reused_block_info['block_number'].max() + 1
        else:
            df_reused_block_info = pd.DataFrame()
            start_block_number = 0

        sql_block_info = SQL_TEMPLATE_BLOCK_INFO.replace('START_BLOCK_NUMBER_PLACEHOLDER', str(start_block_number)).replace('LIMIT_PLACEHOLDER', str(max_count))
        print(f'Scraping block info from block number {start_block_number}')
        records = self.get_all_records(sql_block_info)
        print(f'Scraped {len(records)} block info from block number {start_block_number} to {start_block_number + len(records)}')
        if len(records) == 0: return None
        df_block_info = pd.DataFrame(records)
        df_block_info.columns = df_block_info.columns.str.lower()
        df_block_info.drop_duplicates(subset=['block_number'], inplace=True)
        df_block_info.drop(columns=['__row_index'], inplace=True) if '__row_index' in df_block_info.columns else None
        df_block_info = pd.concat([df_reused_block_info, df_block_info], axis=0)
        if self.if_save:
            df_block_info.to_csv(block_info_file_path, index=False, escapechar='\\')
            df_block_info[['block_number', 'block_timestamp']].to_csv(os.path.join(self.main_dir, 'block_timestamps.csv'), index=False)
            print(f'Saved {len(df_block_info)} block info to {block_info_file_path}')
        return df_block_info

    def scrap_nft_metadata(self, address):
        address = self.ready_for_address(address)
        print(f'Scraping NFT metadata for {address}')
        nft_metadata_file_path = os.path.join(self.chain_dir, address, 'nft_metadata.csv')
        if os.path.exists(nft_metadata_file_path):
            df_nft_metadata = pd.read_csv(nft_metadata_file_path)
            print(f'NFT metadata already scraped: {nft_metadata_file_path}')
            return df_nft_metadata
        sql_template = SQL_TEMPLATE_DICT['nft_metadata']
        sql_nft_metadata = sql_template.replace('ADDRESS_PLACEHOLDER', address)
        records = self.get_all_records(sql_nft_metadata)
        if len(records) == 0: return None
        df_nft_metadata = pd.DataFrame(records)
        df_nft_metadata.columns = df_nft_metadata.columns.str.lower()
        if self.if_save:
            df_nft_metadata.to_csv(nft_metadata_file_path, index=False, escapechar='\\')
            print(f'Saved NFT metadata to {nft_metadata_file_path}')
        return df_nft_metadata

    def scrap_nft_activities(self, activities_type, address, start_block_timestamp=None, end_block_timestamp=None):
        address = self.ready_for_address(address)
        activities_type = activities_type.lower() if activities_type.endswith('s') else activities_type.lower() + 's'
        if 'nft_' + activities_type not in SQL_TEMPLATE_DICT:
            raise Exception(f'Unsupported activities type: {activities_type}')
        else:
            sql_template = SQL_TEMPLATE_DICT['nft_' + activities_type]
        nft_activities_file_path = os.path.join(self.chain_dir, address, f'{activities_type}.csv')
        df_previous_nft_activities = None
        if os.path.exists(nft_activities_file_path):
            df_previous_nft_activities = pd.read_csv(nft_activities_file_path)
            if len(df_previous_nft_activities) > 0:
                max_block_timestamp = df_previous_nft_activities['block_timestamp'].max()
                min_block_timestamp = df_previous_nft_activities['block_timestamp'].min()
                start_block_timestamp = max_block_timestamp
                print(f'Reuse previous {len(df_previous_nft_activities)} {activities_type} records: from {min_block_timestamp} to {max_block_timestamp}')
                # return df_previous_nft_activities
        if start_block_timestamp is None: start_block_timestamp = '2000-01-01'
        if end_block_timestamp is None: end_block_timestamp = self.deadline_timestamp
        end_block_timestamp = '2023-04-08'
        print(f'Scraping {activities_type} records of {address} from {start_block_timestamp} to {end_block_timestamp}')
        sql_query = sql_template.replace('ADDRESS_PLACEHOLDER', address) \
                                .replace('START_BLOCK_TIMESTAMP_PLACEHOLDER', start_block_timestamp) \
                                .replace('END_BLOCK_TIMESTAMP_PLACEHOLDER', end_block_timestamp)
        records = self.get_all_records(sql_query)
        print(f'Scraped new {len(records)} {activities_type} records of {address}')
        if len(records) == 0:
            if df_previous_nft_activities is not None:
                print(f'Saved {activities_type} records to {nft_activities_file_path}')
                return df_previous_nft_activities
            return None
        df_nft_activities = pd.DataFrame(records)
        df_nft_activities.columns = df_nft_activities.columns.str.lower()
        if df_previous_nft_activities is not None:
            df_nft_activities = pd.concat([df_previous_nft_activities, df_nft_activities], axis=0)
        if self.if_save:
            df_nft_activities.to_csv(nft_activities_file_path, index=False, escapechar='\\')
            print(f'Saved {activities_type} records to {nft_activities_file_path}')
        return df_nft_activities

    def scrap_nft_sales(self, address, start_block_timestamp=None, end_block_timestamp=None):
        return self.scrap_nft_activities('sale', address, start_block_timestamp, end_block_timestamp)
    
    def scrap_nft_transfers(self, address, start_block_timestamp=None, end_block_timestamp=None):
        return self.scrap_nft_activities('transfer', address, start_block_timestamp, end_block_timestamp)
    
    def scrap_nft_mints(self, address, start_block_timestamp=None, end_block_timestamp=None):
        return self.scrap_nft_activities('mint', address, start_block_timestamp, end_block_timestamp)

    def scrap_nft_bids(self, address, start_block_timestamp=None, end_block_timestamp=None):
        return self.scrap_nft_activities('bid', address, start_block_timestamp, end_block_timestamp)
    
    def scrap_nft_listings(self, address, start_block_timestamp=None, end_block_timestamp=None):
        return self.scrap_nft_activities('listing', address, start_block_timestamp, end_block_timestamp)


def scrap_nft_metadata_and_activities(address):
        # print(f'Scraping NFT Metadata and Activities of {address}\n')
        # try:
        #     df_nft_metadata = scraper.scrap_nft_metadata(address)
        #     nft_metadata_download_status = len(df_nft_metadata) if df_nft_metadata is not None else 0
        # except:
        #     nft_metadata_download_status = -1
        #     print(f'Failed to scrap NFT metadata of {address}')
        # print()
        nft_activities_download_status_list = []
        for event_type in ['sale', 'mint', 'transfer']:
            nft_activities_download_status = 0
            try:
                df_nft_activities = scraper.scrap_nft_activities(event_type, address)
                nft_activities_download_status = len(df_nft_activities) if df_nft_activities is not None else 0
            except:
                print(f'Failed to scrap NFT {event_type} of {address}')
                nft_activities_download_status = -1
            print()
            nft_activities_download_status_list.append(nft_activities_download_status)
        print(f'='*100)
        return nft_activities_download_status_list

    # df_download_status = pd.DataFrame(download_status_list, columns=['rank', 'address', 'nft_metadata', 'nft_sales', 'nft_mints', 'nft_transfers'])
    # download_status_file_path = f'data/shroomdk/sales_download_status_{chunk_id}.csv'
    # df_download_status.to_csv(download_status_file_path)
    # print(f'Saved download status to {download_status_file_path}')

def retry_download_nft_activities(start_id, end_id):
    df_need_download_status = pd.read_csv('data/shroomdk/need_download_status.csv')
    df_need_download_status = df_need_download_status.loc[start_id:end_id]
    for i, row in df_need_download_status.iterrows():
        if row['nft_mints'] == -1:
            scraper.scrap_nft_mints(row['address'])
            print(f'Scrap the mints of {row["address"]}')
        if row['nft_sales'] == -1:
            scraper.scrap_nft_sales(row['address'])
            print(f'Scrap the sales of {row["address"]}')
        if row['nft_transfers'] == -1:
            scraper.scrap_nft_transfers(row['address'])
            print(f'Scrap the transfers of {row["address"]}')

def aggregate_nft_activities(chain_dir, address, activity_types=['mint', 'sale', 'transfer'], overwrite=False, if_save=True, verbose=False):
    if not os.path.exists(os.path.join(chain_dir, address)):
        print(f'No NFT activities for {address}: {os.path.join(chain_dir, address)}')
        return None
    if not overwrite and os.path.exists(os.path.join(chain_dir, address, 'nft_agg_activities.csv')):
        print(f'NFT activities for {address} already exist: {os.path.join(chain_dir, address, "nft_agg_activities.csv")}')
        # return None
    df_activity_dict = {}
    key_columns = ['block_timestamp', 'block_number', 'source', 'event_type', 
                   'nft_id', 'project_name', 'nft_address', 'tokenid', 'erc1155_value', 
                   'nft_from_address', 'nft_to_address', 'currency_symbol', 'price', 'price_usd']
    for activity_type in activity_types:
        activity_file_name = f'{activity_type}s.csv' if not activity_type.endswith('s') else f'{activity_type}.csv'
        activity_file_path = os.path.join(chain_dir, address, activity_file_name)
        if not os.path.exists(activity_file_path):
            print(f'No {activity_type} activity for {address}: {activity_file_path}')
            df_activity_dict[activity_type] = None
            continue
        df_activity = pd.read_csv(activity_file_path)
        df_activity['block_timestamp'] = pd.to_datetime(df_activity['block_timestamp'])
        if activity_type == 'mint':
            df_activity['source'] = 'nft_mints'
            df_activity['currency_symbol'] = 'ETH'
            df_activity['event_type'] = 'mint'
            df_activity = df_activity.rename(columns={'mint_price_eth': 'price', 'mint_price_usd': 'price_usd'})
        elif activity_type == 'sale':
            df_activity['source'] = 'nft_sales'
            df_activity = df_activity.rename(columns={'seller_address': 'nft_from_address', 'buyer_address': 'nft_to_address'})
            # remove duplicates
            num_sales_before = len(df_activity)
            df_activity = df_activity.sort_values(
                    ['nft_address', 'tokenid', 'block_timestamp', 'nft_from_address', 'nft_to_address', 'price'], 
                    ascending=[True, True, False, True, True, True])
            df_activity = df_activity.drop_duplicates(
                ['nft_address', 'tokenid', 'block_timestamp', 'nft_from_address', 'nft_to_address'], keep='first')
            num_sales_after = len(df_activity)
            df_activity['event_type'] = 'sale'
            print(f'Removed {num_sales_before - num_sales_after} duplicate sales')
        elif activity_type == 'transfer':
            df_activity['source'] = 'nft_transfers'
            df_activity['currency_symbol'] = 'ETH'
            df_activity['price'] = 0.
            df_activity['price_usd'] = 0.
            if df_activity_dict['mint'] is not None and len(df_activity_dict['mint']) > 0:
                num_transfers_before_removing_mints = len(df_activity)
                df_activity = df_activity[df_activity['event_type']!='mint']
                num_transfers_after_removing_mints = len(df_activity)
                print(f'Removed {num_transfers_before_removing_mints - num_transfers_after_removing_mints} transfers that are also mints')
        print(f'Number of {activity_type} activities for {address}: {len(df_activity)}')
        df_activity['nft_id'] = df_activity.apply(lambda x: f'{x.nft_address}_{x.tokenid}', axis=1)
        df_activity = df_activity[key_columns]
        df_activity_dict[activity_type] = df_activity
    if all([df_activity_dict[activity_type] is None for activity_type in activity_types]):
        print(f'No NFT activities for {address}')
        return None
    if df_activity_dict['transfer'] is not None and df_activity_dict['sale'] is not None:
        # if the transfer and sale events overlap, remove the transfer events
        # A -sale-> B: A -transfer-> C -transfer-> B
        df_transfers = df_activity_dict['transfer']
        df_sales = df_activity_dict['sale']
        num_transfers_before_removing_sales = len(df_transfers)
        df_transfers['id'] = df_transfers.apply(lambda x: f'{x.nft_id}_{x.block_timestamp}', axis=1)
        df_sales['id'] = df_sales.apply(lambda x: f'{x.nft_id}_{x.block_timestamp}', axis=1)
        df_transfers = df_transfers[~df_transfers['id'].isin(df_sales['id'])]
        df_transfers = df_transfers.drop(columns=['id'])
        df_sales = df_sales.drop(columns=['id'])
        df_activity_dict['transfer'] = df_transfers
        df_activity_dict['sale'] = df_sales
        num_transfers_after_removing_sales = len(df_transfers)
        print(f'Removed {num_transfers_before_removing_sales - num_transfers_after_removing_sales} transfers that are also sales')
    df_nft_activities = pd.concat(list(df for df in df_activity_dict.values() if df is not None))
    # preprocess
    # update event type
    df_nft_activities.loc[df_nft_activities['source']=='nft_sales', 'event_type'] = 'sale'
    df_nft_activities.loc[df_nft_activities['source']=='nft_mints', 'event_type'] = 'mint'
    df_nft_activities.loc[df_nft_activities['event_type'].isin(['nft_mint']), 'event_type'] = 'mint'
    is_burn_mask = df_nft_activities['nft_to_address'] == '0x0000000000000000000000000000000000000000'
    is_mint_mask = df_nft_activities['event_type'] == 'mint'
    is_transfer_mask = (~is_burn_mask) & (~is_mint_mask)
    df_nft_activities.loc[is_burn_mask & (df_nft_activities['source'] == 'nft_transfers'), 'event_type'] = 'burn'
    df_nft_activities.loc[is_transfer_mask & (df_nft_activities['source'] == 'nft_transfers'), 'event_type'] = 'transfer'
    # remove duplicates
    num_nft_activities_before = len(df_nft_activities)
    df_nft_activities['block_timestamp'] = pd.to_datetime(df_nft_activities['block_timestamp'])
    df_nft_activities = df_nft_activities.sort_values(['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address', 'price'], ascending=[True, False, True, True, True])
    df_nft_activities = df_nft_activities.drop_duplicates(['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address'], keep='first')
    num_nft_activities = len(df_nft_activities)
    print(f'Removed {num_nft_activities_before-num_nft_activities} duplicates: {len(df_nft_activities)} activities')
    # save
    df_nft_activities = df_nft_activities.reset_index(drop=True)
    df_nft_activities = df_nft_activities.sort_values(['block_timestamp'], ascending=[False])
    print(f'Number of activities for {address}: {len(df_nft_activities)}')
    if if_save:
        df_nft_activities.to_csv(os.path.join(chain_dir, address, 'nft_agg_activities.csv'), index=False)
        print(f'NFT activities saved to {os.path.join(chain_dir, address, "nft_agg_activities.csv")}')
    return df_nft_activities

def add_previous_activities(chain_dir, address, overwrite=True, if_save=True):
    file_path = os.path.join(chain_dir, address, 'nft_activities_with_previous_activities.csv')
    nft_activities_file_path = os.path.join(chain_dir, address, 'nft_agg_activities.csv')
    if not os.path.exists(nft_activities_file_path):
        print(f'No activities for {address}: {nft_activities_file_path}')
        return None
    if not overwrite and os.path.exists(file_path):
        print(f'Previous activities already added for {address}: ')
        return None
    df_nft_activities = get_nft_agg_activities_of_collection(chain_dir, address)
    # statistics
    num_nft_activities = len(df_nft_activities)
    num_nft_ids = len(df_nft_activities.nft_id.unique())
    avg_activities_per_id = num_nft_activities / num_nft_ids
    print(f'Averagely activities per id of {address}: {num_nft_activities} / {num_nft_ids} = {avg_activities_per_id} ')
    df_nft_activities = df_nft_activities.sort_values(['nft_id', 'block_timestamp'], ascending=[True, False])
    if (avg_activities_per_id ** 2 * num_nft_ids > 10**7) or num_nft_activities > 10**6:
        BATCH_SIZE = 20
        num_processed = 0
        df_previous_activities_of_nft_id_list = []
        nft_id_list = df_nft_activities['nft_id'].unique().tolist()
        chunk_size = len(nft_id_list) // BATCH_SIZE if len(nft_id_list) // BATCH_SIZE > 0 else 1
        chunk_lists = [nft_id_list[i:i + chunk_size] for i in range(0, len(nft_id_list), chunk_size)]
        for chunk in chunk_lists:
            df_nft_activities_of_nft_id = df_nft_activities[df_nft_activities['nft_id'].isin(chunk)]
            df_previous_activities_of_nft_id = df_nft_activities_of_nft_id[
            ['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address']
        ].rename(columns={
            'block_timestamp': 'previous_block_timestamp', 
            'nft_from_address': 'previous_nft_from_address', 
            'nft_to_address': 'previous_nft_to_address'}).copy()
            df_previous_activities_of_nft_id = df_previous_activities_of_nft_id.merge(
                df_nft_activities_of_nft_id[['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address']], 
                left_on=['nft_id', 'previous_nft_to_address'], 
                right_on=['nft_id', 'nft_from_address'], 
                how='left')
            df_previous_activities_of_nft_id = df_previous_activities_of_nft_id[df_previous_activities_of_nft_id['block_timestamp'] >= df_previous_activities_of_nft_id['previous_block_timestamp']]
            df_previous_activities_of_nft_id = df_previous_activities_of_nft_id.groupby(['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address']).first().reset_index()
            df_previous_activities_of_nft_id_list.append(df_previous_activities_of_nft_id)
            num_processed += len(df_nft_activities_of_nft_id)
            print(f'Processed {num_processed} of {num_nft_activities} NFT activities')
        df_previous_activities = pd.concat(df_previous_activities_of_nft_id_list)
    else:
    # if True:
        df_previous_activities = df_nft_activities[
            ['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address']
        ].rename(columns={
            'block_timestamp': 'previous_block_timestamp', 
            'nft_from_address': 'previous_nft_from_address', 
            'nft_to_address': 'previous_nft_to_address'}).copy()
        df_previous_activities = df_previous_activities.merge(
            df_nft_activities[['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address']], 
            left_on=['nft_id', 'previous_nft_to_address'], 
            right_on=['nft_id', 'nft_from_address'], 
            how='left')
        df_previous_activities = df_previous_activities[df_previous_activities['block_timestamp'] >= df_previous_activities['previous_block_timestamp']]
        df_previous_activities = df_previous_activities.groupby(['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address']).first().reset_index()
        # print('0x4bcc68200c672bc6a5f43a118cabf2507355839f' in df_previous_activities['previous_nft_to_address'].unique().tolist())
    # merge the most previous activity with the original dataframe
    df_activities_with_previous_activities = df_nft_activities.merge(df_previous_activities, on=['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address'], how='left')
    df_activities_with_previous_activities = df_activities_with_previous_activities.merge(
        df_nft_activities, 
        left_on=['nft_id', 'previous_block_timestamp', 'previous_nft_from_address', 'previous_nft_to_address'], 
        right_on=['nft_id', 'block_timestamp', 'nft_from_address', 'nft_to_address'], 
        how='left', 
        suffixes=('', '_previous'))
    df_activities_with_previous_activities = df_activities_with_previous_activities.drop(
        columns=['block_timestamp_previous', 'project_name_previous', 'nft_address_previous', 
                 'tokenid_previous', 'erc1155_value_previous', 
                 'nft_from_address_previous', 'nft_to_address_previous'])
    renamed_columns = {x: 'previous_' + x.replace('_previous', '') for x in df_activities_with_previous_activities.columns if x.endswith('_previous')}
    df_activities_with_previous_activities = df_activities_with_previous_activities.rename(columns=renamed_columns)
    df_activities_with_previous_activities = df_activities_with_previous_activities.sort_values(['nft_id', 'block_timestamp'], ascending=[True, False])
    df_activities_with_previous_activities = df_activities_with_previous_activities.reset_index(drop=True)
    assert len(df_activities_with_previous_activities) == len(df_nft_activities)
    print(f'Completed adding {len(df_activities_with_previous_activities)} previous activities for {address}')
    if if_save:
        df_activities_with_previous_activities.to_csv(file_path, index=False)
        print(f'Saved {len(df_activities_with_previous_activities)} NFT activities with previous activities to {file_path}')
    return df_activities_with_previous_activities

def aggregate_nft_activities_with_previous_activities(chain_dir, address, if_save=True, overwrite=False):
    file_path = os.path.join(chain_dir, address, 'nft_activities_with_previous_activities.csv')
    if not overwrite and os.path.exists(file_path):
        df_nft_activities_with_previous_activities = pd.read_csv(file_path)
        print(f'Loaded {len(df_nft_activities_with_previous_activities)} NFT activities with previous activities from {file_path}')
        return df_nft_activities_with_previous_activities
    df_nft_activities = aggregate_nft_activities(chain_dir, address, if_save=True, overwrite=overwrite)
    df_nft_activities_with_previous_activities = add_previous_activities(chain_dir, address, if_save=if_save, overwrite=overwrite)
    if os.path.exists(os.path.join(chain_dir, address, 'nft_agg_activities.csv')):
        os.remove(os.path.join(chain_dir, address, 'nft_agg_activities.csv'))
        print(f'Removed {len(df_nft_activities)} NFT activities from {os.path.join(chain_dir, address, "nft_agg_activities.csv")}')
    return df_nft_activities_with_previous_activities

def filter_nft_sales_outliers(df_nft_sales_of_collection, window_size=10, verbose=False):
    df_nft_sales_of_collection['block_timestamp'] = pd.to_datetime(df_nft_sales_of_collection['block_timestamp'])
    df_nft_sales_of_collection = df_nft_sales_of_collection.sort_values('block_timestamp', ascending=False)
    window_size = window_size if len(df_nft_sales_of_collection) > window_size else len(df_nft_sales_of_collection)
    rolling_iqr = df_nft_sales_of_collection['price'].rolling(window_size).quantile(0.75) - df_nft_sales_of_collection['price'].rolling(window_size).quantile(0.25)
    rolling_median = df_nft_sales_of_collection['price'].rolling(window_size).median()
    lower_bound = rolling_median - (1.5 * rolling_iqr)
    upper_bound = rolling_median + (1.5 * rolling_iqr)
    lower_bound = lower_bound.fillna(lower_bound.min())
    upper_bound = upper_bound.fillna(upper_bound.max())
    print(f'Lower bound: {lower_bound.min()}') if verbose else None
    print(f'Upper bound: {upper_bound.max()}') if verbose else None
    non_outliers = (df_nft_sales_of_collection['price'] >= lower_bound) & (df_nft_sales_of_collection['price'] <= upper_bound)
    df_nft_sales_of_collection_without_outliers = df_nft_sales_of_collection[non_outliers]
    print(f'Number of outliers: {len(df_nft_sales_of_collection) - len(df_nft_sales_of_collection_without_outliers)}') if verbose else None
    return df_nft_sales_of_collection_without_outliers

def detect_outliers(df_nft_sales_of_collection, window_size=10, verbose=False):
    df_nft_sales_of_collection['block_timestamp'] = pd.to_datetime(df_nft_sales_of_collection['block_timestamp'])
    df_nft_sales_of_collection = df_nft_sales_of_collection.sort_values('block_timestamp', ascending=False)
    window_size = window_size if len(df_nft_sales_of_collection) > window_size else len(df_nft_sales_of_collection)
    rolling_iqr = df_nft_sales_of_collection['price'].rolling(window_size).quantile(0.75) - df_nft_sales_of_collection['price'].rolling(window_size).quantile(0.25)
    rolling_median = df_nft_sales_of_collection['price'].rolling(window_size).median()
    lower_bound = rolling_median - (1.5 * rolling_iqr)
    upper_bound = rolling_median + (1.5 * rolling_iqr)
    lower_bound = lower_bound.fillna(lower_bound.min())
    upper_bound = upper_bound.fillna(upper_bound.max())
    print(f'Upper bound: {upper_bound.max()}, Lower bound: {lower_bound.min()}') if verbose else None
    non_outliers = (df_nft_sales_of_collection['price'] >= lower_bound) & (df_nft_sales_of_collection['price'] <= upper_bound)
    df_nft_sales_of_collection_without_outliers = df_nft_sales_of_collection[non_outliers]
    print(f'Total count: {len(df_nft_sales_of_collection)}, ' + \
            f'Number of outliers: {len(df_nft_sales_of_collection) - len(df_nft_sales_of_collection_without_outliers)}' + \
            f'Remain: {len(df_nft_sales_of_collection_without_outliers)}') if verbose else None
    return df_nft_sales_of_collection_without_outliers

def calculate_nft_estimated_prices_of_data(df_nft_sales_of_collection_data, outliers=False, window_size=10, verbose=False):
    if df_nft_sales_of_collection_data is None or len(df_nft_sales_of_collection_data) == 0:
        print(f'No NFT sales data')
        return None
    df_nft_sales_of_collection_data = df_nft_sales_of_collection_data[df_nft_sales_of_collection_data['currency_symbol'].isin(['ETH', 'WETH'])]
    df_nft_sales_of_collection_data = df_nft_sales_of_collection_data.sort_values('block_timestamp', ascending=True)
    if outliers:
        df_nft_sales_of_collection_data = filter_nft_sales_outliers(df_nft_sales_of_collection_data, window_size=window_size, verbose=verbose)
    df_nft_collection_estimated_prices = df_nft_sales_of_collection_data.groupby(
        df_nft_sales_of_collection_data['block_timestamp'].dt.date)[['price', 'price_usd']].median()
    df_nft_collection_estimated_prices.index = pd.to_datetime(df_nft_collection_estimated_prices.index)
    df_nft_collection_estimated_prices = df_nft_collection_estimated_prices.resample('D').interpolate(method='polynomial', order=1)
    df_nft_collection_estimated_prices = df_nft_collection_estimated_prices.reset_index()
    df_nft_collection_estimated_prices = df_nft_collection_estimated_prices.rename(columns={
        'block_timestamp': 'date', 'price': 'estimated_price', 'price_usd': 'estimated_price_usd'})
    print(f'Number of records: {len(df_nft_collection_estimated_prices)}, std = {df_nft_collection_estimated_prices["estimated_price"].std()}, ' + \
          f'time range from {df_nft_collection_estimated_prices["date"].min()}) to {df_nft_collection_estimated_prices["date"].max()}') if verbose else None
    return df_nft_collection_estimated_prices

def calculate_nft_estimated_prices(chain_dir, address, outliers=False, window_size=10, verbose=False, overwrite=False):
    estimated_price_file_path = os.path.join(chain_dir, address, 'estimated_prices.csv')
    if not overwrite and os.path.exists(estimated_price_file_path):
        df_estimated_price = pd.read_csv(estimated_price_file_path)
        df_estimated_price['date'] = pd.to_datetime(df_estimated_price['date'])
        print(f'Loaded {len(df_estimated_price)} estimated prices from {estimated_price_file_path}')
        return df_estimated_price
    df_nft_sales_of_collection = get_nft_activities_of_collection_by_type(chain_dir, address, 'sales')
    df_nft_collection_estimated_prices = calculate_nft_estimated_prices_of_data(df_nft_sales_of_collection, outliers=outliers, window_size=window_size, verbose=verbose)
    df_nft_collection_estimated_prices['nft_address'] = address
    df_nft_collection_estimated_prices.to_csv(estimated_price_file_path, index=False)
    print(f'Estimated price file saved to {estimated_price_file_path}') if verbose else None
    return df_nft_collection_estimated_prices

def get_file_existence(data_dir, address_list):
    file_name_list = ['mints.csv', 'transfers.csv', 'sales.csv', 'nft_agg_activities.csv', 'nft_activities_with_previous_activities.csv']

    df_file_existence = pd.DataFrame(columns=['address']+[file_name.replace('.csv', '') for file_name in file_name_list])
    for i, address in enumerate(address_list):
        df_file_existence.loc[i, 'address'] = address
        for file_name in file_name_list:
            file_path = os.path.join(chain_dir, address, file_name)
            if not os.path.exists(file_path):
                df_file_existence.loc[address, file_name.replace('.csv', '')] = False
                print(file_path)
            else:
                df_file_existence.loc[address, file_name.replace('.csv', '')] = True

    df_file_existence.to_csv(os.path.join(data_dir, 'file_existence.csv'), index=False)


def scrape_all_collections(address_list, max_workers=10):
    i = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(scrap_nft_metadata_and_activities, collection_address) 
                   for collection_address in address_list]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
                print(f'Finish downloading {i} collections')
            except Exception as exc:
                print(f'Collection processing generated an exception: {exc}')
            finally:
                i += 1

if __name__ == '__main__':
    import os
    
    shroomdk_api_key = os.environ.get('SHROOMDK_API_KEY')
    print(f'='*100)
    main_dir = 'data/shroomdk'
    chain_dir = os.path.join(main_dir, 'ethereum')
    scraper = NftShroomDKScraper(
        api_key=shroomdk_api_key,
        main_dir=main_dir,
        page_size=100000,
        if_save=True,
    )
    """Scraping Blocks"""
    # scraper.scrap_block_info(start_block_number=0)



    """Scraping Top Collections"""
    print(f'='*100)
    start_block_timestamp = '2017-01-01'
    end_block_timestamp = None
    # end_block_timestamp = '2023-04-01'
    scraper.scrap_top_collections(start_block_timestamp=start_block_timestamp, end_block_timestamp=end_block_timestamp, limit=50000)

    # df_top_collections_info = scraper.scrap_top_collections(start_block_timestamp='2017-01-01', end_block_timestamp=end_block_timestamp, limit=10000000)
    """
    
    """
    # final_day_of_year = datetime.date(2023, 1, 1) - datetime.timedelta(days=1)
    # final_day_of_year_str = final_day_of_year.strftime('%Y-%m-%d')
    # start_timestamp = '2022-01-01'
    # end_timestamp = final_day_of_year_str
    # scraper.scrap_collection_daily_volume(start_timestamp=start_timestamp, end_timestamp=final_day_of_year_str)
    # print(f'='*100)


    # """Collection-level Functions"""
    # df_top_collections_info = pd.read_csv('data/shroomdk/top_collections_info_2023-03-01_2023-04-10.csv')  
    # # top_collection_address_list = df_top_collections_info[:1000]['address'].tolist()
    # df_top_collections_info = pd.read_csv('data/shroomdk/top_10000000_collections_info_2017-01-01_2023-04-01.csv')  
    # top_collection_address_list = df_top_collections_info[1000:1001]['address'].tolist()
    # print(f'Number of collections: {len(top_collection_address_list[:])}')

    # """
    # Parallel
    # """
    # max_workers = 4
    # i = 0
    # with ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     futures = [executor.submit(scrap_nft_metadata_and_activities, collection_address) 
    #                for collection_address in top_collection_address_list]
    #     for future in concurrent.futures.as_completed(futures):
    #         try:
    #             future.result()
    #             print(f'Finish downloading {i} collections')
    #         except Exception as exc:
    #             print(f'Collection processing generated an exception: {exc}')
    #         finally:
    #             i += 1