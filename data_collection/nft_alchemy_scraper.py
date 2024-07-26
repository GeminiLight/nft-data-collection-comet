import os
import ast
import json
import time
import tqdm
import httpx
import pprint
import yaml
import datetime
import numpy as np
import pandas as pd


def read_json(fpath, mode='r'):
    with open(fpath, mode, encoding='utf-8') as f:
        if fpath[-4:] == 'json':
            setting_dict = json.load(f)
        else:
            return ValueError('Only supports settings files in yaml and json format!')
    return setting_dict

def write_json(setting_dict, fpath, mode='w'):
    with open(fpath, mode, encoding='utf-8') as f:
        if fpath[-4:] == 'json':
            json.dump(setting_dict, f, indent=4)
        elif fpath[-4:] == 'yaml':
            yaml.dump(setting_dict, f)
        else:
            return ValueError('Only supports settings files in yaml and json format!')
    return setting_dict

def save_json(setting_dict, fpath, mode='w'):
    return write_json(setting_dict, fpath, mode)


def ensure_dataframe(data):
    if isinstance(data, list):
        df_data = pd.DataFrame(data)
    elif isinstance(data, pd.DataFrame):
        df_data = data
    else:
        raise ValueError('data must be list or pd.DataFrame')
    return df_data



class JsonToCsvConverter:

    def __init__(self):
        pass
    
    @classmethod
    def convert_contract_metadata(self, contract_metadata_list):
        df_raw_contract_metadata = pd.DataFrame(contract_metadata_list)
        # Process contract metadata
        df_contract_metadata = pd.DataFrame()
        df_contract_metadata['address'] = df_raw_contract_metadata['address']
        df_contract_metadata['name'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('name', None))
        df_contract_metadata['symbol'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('symbol', None))
        df_contract_metadata['total_supply'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('totalSupply', None))
        df_contract_metadata['token_type'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('tokenType', None))
        df_contract_metadata['contract_deployer'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('contractDeployer', None))
        df_contract_metadata['deployed_block_number'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('deployedBlockNumber', None))

        df_contract_metadata['image_url'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('imageUrl', None))
        df_contract_metadata['description'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('description', None))
        df_contract_metadata['external_url'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('externalUrl', None))
        df_contract_metadata['twitter_username'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('twitterUsername', None))
        df_contract_metadata['banner_image_url'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('bannerImageUrl', None))
        df_contract_metadata['open_sea_floor_price'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('floorPrice', None))
        df_contract_metadata['open_sea_collection_name'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('collectionName', None))
        df_contract_metadata['open_sea_collection_slug'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('collectionSlug', None))
        df_contract_metadata['open_sea_safelist_request_status'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('safelistRequestStatus', None))
        df_contract_metadata['open_sea_last_ingested_at'] = df_raw_contract_metadata['contractMetadata'].apply(lambda x: x.get('openSea', {}).get('lastIngestedAt', None))
        return df_contract_metadata


    @classmethod
    def convert_token_metadata(self, token_metadata_list):
        df_raw_token_metadata = ensure_dataframe(token_metadata_list)
        # Process token metadata
        df_token_metadata = pd.DataFrame()
        df_token_metadata['contract_address'] = df_raw_token_metadata['contract'].apply(lambda x: x.get('address', None))
        df_token_metadata['token_id'] = df_raw_token_metadata['id'].apply(lambda x: int(x['tokenId'], 16) if x['tokenId'].startswith('0x') else x)   # if 'tokenId' in x else None
        df_token_metadata['token_type'] = df_raw_token_metadata['id'].apply(lambda x: x['tokenMetadata'].get('tokenType', None))
        df_token_metadata['title'] = df_raw_token_metadata['title'].astype(str).str.replace('\n', ' ')
        df_token_metadata['title'] = df_raw_token_metadata['title'].astype(str).str.replace('\r', ' ')
        df_token_metadata['properties'] = df_raw_token_metadata['metadata'].apply(lambda x: x.get('attributes', None) if type(x) == dict else None)
        df_token_metadata['description'] = df_raw_token_metadata['description'].astype(str).str.replace('\n', ' ')
        df_token_metadata['description'] = df_raw_token_metadata['description'].astype(str).str.replace('\r', ' ')
        df_token_metadata['token_uri_raw'] = df_raw_token_metadata['tokenUri'].apply(lambda x: x.get('raw', None))
        df_token_metadata['token_uri_gateway'] = df_raw_token_metadata['tokenUri'].apply(lambda x: x.get('gateway', None))
        df_token_metadata['media_url'] = df_raw_token_metadata['media'].apply(lambda x: x[0].get('gateway', None) if len(x) > 0 else None)
        df_token_metadata['media_url_gateway'] = df_raw_token_metadata['media'].apply(lambda x: x[0].get('gateway', None) if len(x) > 0 else None)
        df_token_metadata['media_url_thumbnail'] = df_raw_token_metadata['media'].apply(lambda x: x[0].get('thumbnail', None) if len(x) > 0 else None)
        df_token_metadata['media_url_raw'] = df_raw_token_metadata['media'].apply(lambda x: x[0].get('raw', None) if len(x) > 0 else None)
        df_token_metadata['media_format'] = df_raw_token_metadata['media'].apply(lambda x: x[0].get('format', None) if len(x) > 0 else None)
        df_token_metadata['media_bytes'] = df_raw_token_metadata['media'].apply(lambda x: x[0].get('bytes', None) if len(x) > 0 else None)
        df_token_metadata['last_update_time'] = df_raw_token_metadata['timeLastUpdated']
        return df_token_metadata
    
    @classmethod
    def convert_token_sales(self, token_sale_list):
        df_raw_token_sales = ensure_dataframe(token_sale_list)
        df_token_sales = pd.DataFrame()
        df_token_sales['marketplace'] = df_raw_token_sales['marketplace']
        df_token_sales['marketplace_address'] = df_raw_token_sales['marketplaceAddress']
        df_token_sales['contract_address'] = df_raw_token_sales['contractAddress']
        df_token_sales['token_id'] = df_raw_token_sales['tokenId']
        df_token_sales['quantity'] = df_raw_token_sales['quantity']
        df_token_sales['buyer_address'] = df_raw_token_sales['buyerAddress']
        df_token_sales['seller_address'] = df_raw_token_sales['sellerAddress']
        df_token_sales['taker'] = df_raw_token_sales['taker']
        df_token_sales['seller_fee_amount'] = df_raw_token_sales['sellerFee'].apply(lambda x: x.get('amount', None))
        df_token_sales['seller_fee_token_address'] = df_raw_token_sales['sellerFee'].apply(lambda x: x.get('tokenAddress', None))
        df_token_sales['seller_fee_symbol'] = df_raw_token_sales['sellerFee'].apply(lambda x: x.get('symbol', None))
        df_token_sales['seller_fee_decimals'] = df_raw_token_sales['sellerFee'].apply(lambda x: x.get('decimals', None))
        df_token_sales['protocol_fee_amount'] = df_raw_token_sales['protocolFee'].apply(lambda x: x.get('amount', None))
        df_token_sales['protocol_fee_token_address'] = df_raw_token_sales['protocolFee'].apply(lambda x: x.get('tokenAddress', None))
        df_token_sales['protocol_fee_symbol'] = df_raw_token_sales['protocolFee'].apply(lambda x: x.get('symbol', None))
        df_token_sales['protocol_fee_decimals'] = df_raw_token_sales['protocolFee'].apply(lambda x: x.get('decimals', None))
        df_token_sales['royalty_fee_amount'] = df_raw_token_sales['royaltyFee'].apply(lambda x: x.get('amount', None))
        df_token_sales['royalty_fee_token_address'] = df_raw_token_sales['royaltyFee'].apply(lambda x: x.get('tokenAddress', None))
        df_token_sales['royalty_fee_symbol'] = df_raw_token_sales['royaltyFee'].apply(lambda x: x.get('symbol', None))
        df_token_sales['royalty_fee_decimals'] = df_raw_token_sales['royaltyFee'].apply(lambda x: x.get('decimals', None))
        df_token_sales['block_number'] = df_raw_token_sales['blockNumber']
        df_token_sales['transaction_hash'] = df_raw_token_sales['transactionHash']
        df_token_sales['log_index'] = df_raw_token_sales['logIndex']
        df_token_sales['bundle_index'] = df_raw_token_sales['bundleIndex']

        def convert_fee_amount(df, symbol_column, amount_column, symbol_list, conversion_factor):
            symbol_filter = df[symbol_column].isin(symbol_list) & ~df[amount_column].isin([None, '', 'null'])
            df.loc[symbol_filter, amount_column] = df.loc[symbol_filter, amount_column].apply(lambda x: float(x) / conversion_factor)

        eth_weth_symbols = ['ETH', 'WETH']
        usdc_symbols = ['USDC']
        dai_symbols = ['DAI']
        ape_symbols = ['APE']
        eth_weth_conversion_factor = 10**18
        usdc_conversion_factor = 10**6
        dai_conversion_factor = 10**18
        ape_conversion_factor = 10**18

        fee_types = [
            ('seller_fee_symbol', 'seller_fee_amount', eth_weth_symbols, eth_weth_conversion_factor),
            ('seller_fee_symbol', 'seller_fee_amount', usdc_symbols, usdc_conversion_factor),
            ('seller_fee_symbol', 'seller_fee_amount', dai_symbols, dai_conversion_factor),
            ('seller_fee_symbol', 'seller_fee_amount', ape_symbols, ape_conversion_factor),
            ('royalty_fee_symbol', 'royalty_fee_amount', eth_weth_symbols, eth_weth_conversion_factor),
            ('royalty_fee_symbol', 'royalty_fee_amount', usdc_symbols, usdc_conversion_factor),
            ('royalty_fee_symbol', 'royalty_fee_amount', dai_symbols, dai_conversion_factor),
            ('royalty_fee_symbol', 'royalty_fee_amount', ape_symbols, ape_conversion_factor),
            ('protocol_fee_symbol', 'protocol_fee_amount', eth_weth_symbols, eth_weth_conversion_factor),
            ('protocol_fee_symbol', 'protocol_fee_amount', usdc_symbols, usdc_conversion_factor),
            ('protocol_fee_symbol', 'protocol_fee_amount', dai_symbols, dai_conversion_factor),
            ('protocol_fee_symbol', 'protocol_fee_amount', ape_symbols, ape_conversion_factor),
        ]

        for fee_type in fee_types:
            convert_fee_amount(df_token_sales, *fee_type)

        return df_token_sales

    @classmethod
    def convert_token_transfers(self, token_transfer_list):
        df_raw_token_transfers = ensure_dataframe(token_transfer_list)
        # df_raw_token_transfers_token_id_is_none = df_raw_token_transfers[df_raw_token_transfers['erc1155Metadata'].isin([None, '', 'null', []])]
        # df_raw_token_transfers_token_id_is_none.to_csv('token_transfers_token_id_is_none.csv', index=False)
        erc721_filter = df_raw_token_transfers['category'] == 'erc721'
        erc1155_filter = df_raw_token_transfers['category'] == 'erc1155'
        erc1155_not_valid_filter = (erc1155_filter & df_raw_token_transfers['erc1155Metadata'].isin([None, '', 'null', []]))
        erc1155_valid_filter = (erc1155_filter & ~df_raw_token_transfers['erc1155Metadata'].isin([None, '', 'null', []]))
        df_token_transfers = pd.DataFrame()
        df_token_transfers['block_number'] = df_raw_token_transfers['blockNum'].apply(lambda x: int(x, 16) if x.startswith('0x') else x)
        df_token_transfers['transaction_hash'] = df_raw_token_transfers['hash']
        df_token_transfers['unique_id'] = df_raw_token_transfers['uniqueId']
        df_token_transfers['from_address'] = df_raw_token_transfers['from']
        df_token_transfers['to_address'] = df_raw_token_transfers['to']
        df_token_transfers.loc[erc721_filter, 'token_id'] = df_raw_token_transfers.loc[erc721_filter, 'tokenId'].apply(lambda x: int(x, 16) if x.startswith('0x') else x)
        df_token_transfers.loc[erc1155_valid_filter, 'token_id'] = df_raw_token_transfers.loc[erc1155_valid_filter, 'erc1155Metadata'].apply(
                                                                lambda x: int(x[0]['tokenId'], 16) if x[0]['tokenId'].startswith('0x') else x)
        df_token_transfers.loc[erc1155_not_valid_filter, 'token_id'] = np.nan
        df_token_transfers.loc[erc721_filter, 'value'] = df_raw_token_transfers.loc[erc721_filter, 'value']
        df_token_transfers.loc[erc1155_valid_filter, 'value'] = df_raw_token_transfers.loc[erc1155_valid_filter, 'erc1155Metadata'].apply(
                                                                lambda x: int(x[0]['value'], 16) if x[0]['value'].startswith('0x') else x)
        df_token_transfers.loc[erc1155_not_valid_filter, 'value'] = np.nan
        df_token_transfers['category'] = df_raw_token_transfers['category']
        df_token_transfers['asset'] = df_raw_token_transfers['asset']
        df_token_transfers['contract_address'] = df_raw_token_transfers['rawContract'].apply(lambda x: x.get('address', None))
        df_token_transfers['contract_value'] = df_raw_token_transfers['rawContract'].apply(lambda x: x.get('value', None))
        df_token_transfers['contract_decimal'] = df_raw_token_transfers['rawContract'].apply(lambda x: x.get('decimal', None))
        df_token_transfers = df_token_transfers.drop_duplicates(subset=['unique_id'])
        return df_token_transfers


class NftAlchemyScraper:

    def __init__(
            self, 
            api_key, 
            chain='ETHEREUM', 
            main_save_dir='data/alchemy_nft_data',
            limit=100, 
            sleep_time=0.1, 
            if_save=True, 
            if_reuse=True,
        ):
        assert limit <= 100 and limit > 0
        self.api_key = api_key
        self.base_nft_url = 'https://eth-mainnet.g.alchemy.com/nft/v2/' + api_key + '/'
        self.base_main_url = 'https://eth-mainnet.g.alchemy.com/v2/' + api_key + '/'
        self.connect_timeout = 60 * 2
        self.read_timeout = 120 * 2
        # self.client = httpx.Client(headers={"accept": "application/json"}, timeout=(self.connect_timeout, self.read_timeout))
        self.limit = limit
        self.sleep_time = sleep_time
        self.main_save_dir = main_save_dir
        self.chain_save_dir = os.path.join(self.main_save_dir, chain)
        self.page_key_file_name = 'token_sales_page_key.txt'
        self.token_metadata_file_name = 'token_metadata'
        self.token_properties_file_name = 'token_properties'
        self.token_sales_file_name = 'token_sales'
        self.token_transfers_file_name = 'token_transfers'
        self.contract_metadata_file_name = 'contract_metadata'
        self.eth_block_info_file_name = 'eth_block_info'
        self.eth_transaction_to_block_file_name = 'eth_transaction_to_block'
        self.rarity_summary_file_name = 'rarity_summary'
        self.if_save = if_save
        self.if_reuse = if_reuse
        self.json_to_csv_converter = JsonToCsvConverter()
        for save_dir in [self.main_save_dir, self.chain_save_dir]:
            if not os.path.exists(save_dir):
                os.makedirs(save_dir, exist_ok=True)

    def _ready_for_contract(self, contract_address):
        contract_address = contract_address.lower()
        if self.if_save:
            self.contract_save_dir = os.path.join(self.chain_save_dir, contract_address)
            if not os.path.exists(self.contract_save_dir):
                os.makedirs(self.contract_save_dir)
        return contract_address

    def _read_page_key(self, page_key_file_path):
        if os.path.exists(page_key_file_path):
            with open(page_key_file_path, 'r') as f:
                page_key = f.read()
            if page_key in ['', 'None', 'null']:
                page_key = None
        else:
            page_key = None
        return page_key

    def _request(self, url, method='get', json=None):
        assert method in ['get', 'post']
        headers = {"accept": "application/json"} if method == 'get' else {"accept": "application/json", "content-type": "application/json"}
        res = httpx.request(method=method, url=url, headers=headers, json=json, timeout=(self.connect_timeout, self.read_timeout))
        res_status_code = res.status_code
        if res_status_code == 200:
            res_data = res.json()
        else:
            res_data = None
        return res_status_code, res_data

    def _ensure_dataframe(self, data):
        if isinstance(data, list):
            df_data = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df_data = data
        else:
            raise ValueError('data must be list or pd.DataFrame')
        return df_data

    def scrap_eth_block_info(self, max_block_number=1000):
        eth_block_info_file_path = os.path.join(self.main_save_dir, self.eth_block_info_file_name+'.csv')
        if self.if_reuse and os.path.exists(eth_block_info_file_path):
            df_reused_eth_block_info = pd.read_csv(eth_block_info_file_path)
            next_block_number = df_reused_eth_block_info['number'].max() + 1
            print('Reused block info from:', eth_block_info_file_path)
        else:
            df_reused_eth_block_info = pd.DataFrame()
            next_block_number = 0

        print('Start scraping block info from block number:', next_block_number)
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(next_block_number), False]
        }
        newly_block_info = []
        request_count = 0
        pbar = tqdm.tqdm(desc='Scraping block info', total=max_block_number)
        while True:
            payload['params'][0] = hex(next_block_number)
            res_status_code, res_data = self._request(self.base_main_url, method='post', json=payload)
            if res_status_code == 200:
                if res_data['result'] is None:
                    break
                block_info = res_data['result']
                block_info.pop('transactions')
                block_info.pop('logsBloom')
                block_info['number'] = int(block_info['number'], 16)
                block_info['timestamp'] = int(block_info['timestamp'], 16)
                block_info['timestamp'] = datetime.datetime.fromtimestamp(block_info['timestamp']).strftime("%Y-%m-%dT%H:%M:%S")
                newly_block_info.append(block_info)
                next_block_number += 1
                request_count += 1
                pbar.update(1)
            else:
                break

            if next_block_number > max_block_number:
                break

        if len(newly_block_info) > 0:
            df_newly_block_info = pd.DataFrame(newly_block_info)
            df_eth_block_info = pd.concat([df_reused_eth_block_info, df_newly_block_info], axis=0)
            if self.if_save:
                df_eth_block_info.to_csv(eth_block_info_file_path, index=False)

        return df_eth_block_info

    def scrap_contract_metadata(self, contract_address_list):
        contract_address_list = [contract_address.lower() for contract_address in contract_address_list]
        api_name = 'getContractMetadataBatch'
        url = self.base_nft_url + api_name
        print('Expected number of collections:', len(contract_address_list))
        if len(contract_address_list) > self.limit:
            sub_contract_address_list_indices = [i for i in range(0, len(contract_address_list), self.limit)]
            if sub_contract_address_list_indices[-1] != len(contract_address_list):
                sub_contract_address_list_indices.append(len(contract_address_list))
        else:
            sub_contract_address_list_indices = [0, len(contract_address_list)]
        collection_metadata_list = []
        pbar = tqdm.tqdm(desc='Scraping metadata', total=len(sub_contract_address_list_indices)-1)
        for i in range(len(sub_contract_address_list_indices)-1):
            sub_contract_address_list = contract_address_list[sub_contract_address_list_indices[i]:sub_contract_address_list_indices[i+1]]
            payload = {'contractAddresses': sub_contract_address_list}
            res_status_code, res_data = self._request(url, method='post', json=payload)
            pbar.update(1)
            pbar.set_description_str(f'{i:05d} - Scraped metadata {sub_contract_address_list_indices[i]:05d} to {sub_contract_address_list_indices[i+1]:05d}: status_code={res_status_code}')
            # print(f'{i:05d} - Scraped metadata {sub_contract_address_list_indices[i]:05d} to {sub_contract_address_list_indices[i+1]:05d}: status_code={res_status_code}')
            if res_status_code == 200:
                collection_metadata_list += res_data
            time.sleep(self.sleep_time)
        pbar.close()
        if len(collection_metadata_list) == 0:
            return None
        print('Actual Downloaded number of collections:', len(collection_metadata_list))
        df_contract_metadata = pd.DataFrame(collection_metadata_list)
        if self.if_save:
            contract_metadata_file_path = os.path.join(self.main_save_dir, self.contract_metadata_file_name+'.json')
            df_contract_metadata.to_json(contract_metadata_file_path, orient='records', indent=4)
            print('Saved to:', os.path.join(self.main_save_dir, self.contract_metadata_file_name+'.json'))
            df_contract_metadata_for_csv = self.json_to_csv_converter.convert_contract_metadata(collection_metadata_list)
            df_contract_metadata_for_csv.to_csv(os.path.join(self.main_save_dir, self.contract_metadata_file_name+'.csv'), index=False, escapechar='\\')
            print('Saved to:', os.path.join(self.main_save_dir, self.contract_metadata_file_name+'.csv'))
        return collection_metadata_list
    
    def scrap_contract_token_metadata(self, contract_address, startToken=0, with_metadata=True, sample=False):
        contract_address = self._ready_for_contract(contract_address)
        if not sample:
            metadata_json_file_path = os.path.join(self.contract_save_dir, self.token_metadata_file_name+'.json')
            metadata_csv_file_path = os.path.join(self.contract_save_dir, self.token_metadata_file_name+'.csv')
            token_properties_file_path = os.path.join(self.contract_save_dir, self.token_properties_file_name+'.csv')
        else:
            metadata_json_file_path = os.path.join(self.contract_save_dir, self.token_metadata_file_name+'_sample.json')
            metadata_csv_file_path = os.path.join(self.contract_save_dir, self.token_metadata_file_name+'_sample.csv')
            token_properties_file_path = os.path.join(self.contract_save_dir, self.token_properties_file_name+'_sample.csv')
        
        is_exist_metadata_json_file = os.path.exists(metadata_json_file_path)
        is_exist_metadata_csv_file = os.path.exists(metadata_csv_file_path)
        is_exist_token_properties_file = os.path.exists(token_properties_file_path)
        if self.if_reuse:
            if os.path.exists(metadata_csv_file_path):
                df_token_metadata = pd.read_csv(is_exist_metadata_csv_file)
                print(f'Reused {len(df_token_metadata)} metadata from:', is_exist_metadata_csv_file)
                if os.path.exists(token_properties_file_path):
                    df_token_properties = pd.read_csv(token_properties_file_path)
                    # print(f'Reused {len(df_token_properties)} token properties from:', token_properties_file_path)
                else:
                    df_token_properties = extract_token_properties_from_metadata_csv(df_token_metadata)
                    if df_token_properties is not None:
                        df_token_properties.to_csv(token_properties_file_path, index=False, escapechar='\\') 
                        print('Saved to:', token_properties_file_path)
                return df_token_metadata, df_token_properties

        api_name = 'getNFTsForCollection'
        request_params = f'contractAddress={contract_address}&withMetadata={str(with_metadata).lower()}&limit={self.limit}'
        request_url = f'{self.base_nft_url+api_name}?{request_params}'

        token_metadata_list = []
        token_count, request_count = 0, 0
        while request_url:
            res_status_code, res_data = self._request(request_url)
            if res_status_code == 500:
                print(f'Error: status_code={res_status_code}, {contract_address}')
                return None, None
            if res_status_code != 200 and res_status_code != 500:
                raise Exception(f'Error: status_code={res_status_code}, {contract_address}')
            token_metadata_list += res_data['nfts']
            request_count += 1
            token_count += len(res_data['nfts'])
            if request_count % 100 == 0:
                print(f'{request_count:05d} - Scraped {len(token_metadata_list)} metadata ({startToken:05d} to {startToken+self.limit:05d}): status_code={res_status_code}')
            time.sleep(self.sleep_time)
            # judge if there are more tokens
            if 'nextToken' in res_data:
                startToken = int(res_data["nextToken"], 16)
                startTokenHex = res_data['nextToken']
                request_url = f'{self.base_nft_url+api_name}?{request_params}&startToken={startTokenHex}'
            else:
                request_url = None
            # if sample mode is on, break the loop
            if sample:
                break
        if len(token_metadata_list) == 0:
            return None, None
        print('Downloaded number of tokens:', len(token_metadata_list))
        df_token_metadata = pd.DataFrame(token_metadata_list)
        if self.if_save:
            # df_token_metadata.to_json(metadata_json_file_path, orient='records', indent=4)
            # print('Saved to:', metadata_json_file_path)
            df_token_metadata_for_csv = self.json_to_csv_converter.convert_token_metadata(df_token_metadata)
            df_token_metadata_for_csv.to_csv(metadata_csv_file_path, index=False, escapechar='\\')
            print('Saved to:', metadata_json_file_path)
            df_token_properties = extract_token_properties_from_metadata_csv(df_token_metadata_for_csv)
            if df_token_properties is not None:
                df_token_properties.to_csv(token_properties_file_path, index=False, escapechar='\\') 
                print('Saved to:', token_properties_file_path)

        return df_token_metadata, df_token_properties

    def scrap_contract_token_sales(self, contract_address, from_block=0, to_block='latest'):
        contract_address = self._ready_for_contract(contract_address)
        num_reused_sales = 0
        sales_csv_file_path = os.path.join(self.contract_save_dir, self.token_sales_file_name+'.csv')

        if self.if_reuse and os.path.exists(sales_csv_file_path):
            df_reused_sales = pd.read_csv(sales_csv_file_path)
            num_reused_sales = len(df_reused_sales)
            print(f'Reused {num_reused_sales} sales from: {sales_csv_file_path}')
            print(f'Reused Block Number from {df_reused_sales.iloc[0]["block_number"]} to {df_reused_sales.iloc[-1]["block_number"]}')
        else:
            df_reused_sales = pd.DataFrame()
            num_reused_sales = 0
        latest_block_number = df_reused_sales.iloc[-1]['block_number'] if num_reused_sales > 0 else '0x0'
        limit = int(self.limit * 10)
        request_params = f'fromBlock={latest_block_number}&toBlock={to_block}&order=asc&contractAddress={contract_address}&limit={limit}'
        request_url = f'{self.base_nft_url+"getNFTSales"}?{request_params}'
        sale_count, request_count = 0, 0
        newly_sales = []
        page_key = ''
        while request_url:
            res_status_code, res_data = self._request(request_url)
            if res_status_code != 200:
                raise Exception(f'Error: status_code={res_status_code}')
            else:
                one_request_sales = res_data['nftSales']
            # filter out the duplicated sales
            sale_count = 0
            newly_oldest_block_number = one_request_sales[-1]['blockNumber'] if len(one_request_sales) > 0 else ''
            if newly_oldest_block_number == latest_block_number:
                likely_dup_sales_id = df_reused_sales[df_reused_sales['block_number'] == latest_block_number]['transaction_hash'].tolist()
                for sale in one_request_sales:
                    if sale['transactionHash'] not in likely_dup_sales_id:
                        newly_sales.append(sale)
                        sale_count += 1
            else:
                newly_sales += one_request_sales
                sale_count += len(one_request_sales)
            # statistics and print
            request_count += 1
            if len(one_request_sales) > 0:
                print(f'{request_count:05d} - Scraped {len(newly_sales):04d} sales (Batch {sale_count:04d}): ' + \
                      f'{one_request_sales[0]["blockNumber"]} to {one_request_sales[-1]["blockNumber"]}, ' + \
                      f'pageKey={page_key}, status_code={res_status_code}')
            # judge if there are more sales
            if 'pageKey' in res_data:
                page_key = res_data["pageKey"]
                request_url = f'{self.base_nft_url+"getNFTSales"}?{request_params}&pageKey={page_key}'
            else:
                request_url = None
        # merge reused and newly downloaded sales
        if len(newly_sales) > 0:
            df_raw_newly_sales = pd.DataFrame(newly_sales)
            df_newly_sales = self.json_to_csv_converter.convert_token_sales(df_raw_newly_sales)
            df_token_sales = pd.concat([df_reused_sales, df_newly_sales], ignore_index=True)
        else:
            df_token_sales = df_reused_sales
        if len(df_token_sales) == 0:
            print(f'Total {len(df_token_sales)} sales')
            return None
        df_token_sales = df_token_sales.drop_duplicates(subset=['transaction_hash'])
        num_new_sales = len(df_token_sales) - num_reused_sales
        print(f'Newly Downloaded {num_new_sales} sales')
        print(f'Total {len(df_token_sales)} sales: from {df_token_sales["block_number"].iloc[-1]} to {df_token_sales["block_number"].iloc[0]}')
        if self.if_save and num_new_sales > 0:
            df_token_sales.to_csv(os.path.join(self.contract_save_dir, self.token_sales_file_name+'.csv'), index=False, escapechar='\\')
            print('Saved to:', os.path.join(self.contract_save_dir, self.token_sales_file_name+'.csv'))
        return df_token_sales

    def scrap_contract_token_transfers(self, contract_address, from_block=0, to_block='latest'):
        contract_address = self._ready_for_contract(contract_address)
        transfers_csv_file_path = os.path.join(self.contract_save_dir, self.token_transfers_file_name+'.csv')
        if self.if_reuse and os.path.exists(transfers_csv_file_path):
            df_reused_transfers = pd.read_csv(transfers_csv_file_path)
            num_reused_transfers = len(df_reused_transfers)
            print(f'Reused {num_reused_transfers} transfers from: {transfers_csv_file_path}')
            print(f'Reused Block Number from {df_reused_transfers.iloc[0]["block_number"]} to {df_reused_transfers.iloc[-1]["block_number"]}')
        else:
            df_reused_transfers = pd.DataFrame()
            num_reused_transfers = 0
        latest_block_number = df_reused_transfers.iloc[-1]['block_number'] if num_reused_transfers > 0 else '0x0'
        limit = int(self.limit * 10)
        payload = {
            'id': 1,
            'jsonrpc': '2.0',
            'method': 'alchemy_getAssetTransfers',
            'params': [
                {
                    'fromBlock': latest_block_number,
                    'toBlock': 'latest',
                    'category': ['erc721', 'erc1155', 'specialnft'],
                    'withMetadata': False,
                    'excludeZeroValue': True,
                    'maxCount': hex(limit),
                    'contractAddresses': [contract_address],
                    'order': 'asc'
                }
            ]
        }
        request_count = 0
        newly_transfers = []
        page_key = ''
        request_url = self.base_main_url
        while request_url:
            res_status_code, res_data = self._request(request_url, method='post', json=payload)
            if res_status_code != 200:
                raise Exception(f'Error: status_code={res_status_code}')
            else:
                one_request_transfers = res_data['result']['transfers']
            # filter out the duplicated transfers
            transfer_count = 0
            newly_oldest_block_number = one_request_transfers[-1]['blockNum'] if len(one_request_transfers) > 0 else ''
            if newly_oldest_block_number == latest_block_number:
                likely_dup_transfers_id = df_reused_transfers[df_reused_transfers['block_number'] == latest_block_number]['unique_id'].tolist()
                for transfer in one_request_transfers:
                    if transfer['uniqueId'] not in likely_dup_transfers_id:
                        newly_transfers.append(transfer)
                        transfer_count += 1
            else:
                newly_transfers += one_request_transfers
                transfer_count += len(one_request_transfers)
            # statistics and print
            request_count += 1
            if len(one_request_transfers) > 0:
                print(f'{request_count:05d} - Scraped {len(newly_transfers):04d} transfers (Batch {transfer_count:04d}): ' + \
                      f'{one_request_transfers[0]["blockNum"]} to {one_request_transfers[-1]["blockNum"]}, ' + \
                      f'pageKey={page_key}, status_code={res_status_code}')
            # judge if there are more transfers
            if 'pageKey' in res_data['result']:
                page_key = res_data['result']['pageKey']
                payload['params'][0]['pageKey'] = page_key
                request_url = self.base_main_url
            else:
                request_url = None
        # merge reused and newly downloaded transfers
        if len(newly_transfers) > 0:
            df_raw_newly_transfers = pd.DataFrame(newly_transfers)
            df_newly_transfers = self.json_to_csv_converter.convert_token_transfers(df_raw_newly_transfers)
            df_token_transfers = pd.concat([df_reused_transfers, df_newly_transfers], ignore_index=True)
        else:
            df_token_transfers = df_reused_transfers
        if len(df_token_transfers) == 0:
            print(f'Total {len(df_token_transfers)} transfers')
            return None
        df_token_transfers = df_token_transfers.drop_duplicates(subset=['unique_id'])
        # df_token_transfers = df_token_transfers.sort_values(by=['block_number'], ascending=True)
        num_new_transfers = len(df_token_transfers) - num_reused_transfers
        print(f'Newly Downloaded {num_new_transfers} transfers')
        print(f'Total {len(df_token_transfers)} transfers: from {df_token_transfers["block_number"].iloc[-1]} to {df_token_transfers["block_number"].iloc[0]}')
        if self.if_save and num_new_transfers > 0:
            df_token_transfers.to_csv(os.path.join(self.contract_save_dir, self.token_transfers_file_name+'.csv'), index=False, escapechar='\\')
            print('Saved to:', os.path.join(self.contract_save_dir, self.token_transfers_file_name+'.csv'))
        return df_token_transfers

    def scrap_contract_rarity_summary(self, contract_address):
        contract_address = self._ready_for_contract(contract_address)
        rarity_summary_file_path = os.path.join(self.contract_save_dir, self.rarity_summary_file_name+'.json')
        if self.if_reuse:
            if os.path.exists(rarity_summary_file_path):
                rarity_summary = read_json(rarity_summary_file_path)
                print(f'Reused rarity summary from: {rarity_summary_file_path}')
                return rarity_summary
        contract_address = self._ready_for_contract(contract_address)
        api_name = 'summarizeNFTAttributes'
        request_params = f'contractAddress={contract_address}'
        request_url = f'{self.base_nft_url+api_name}?{request_params}'
        res_status_code, res_data = self._request(request_url)
        if res_status_code != 200:
            return None
        rarity_summary = res_data
        if self.if_save:
            save_json(rarity_summary, os.path.join(rarity_summary_file_path))
            print('Saved to:', os.path.join(rarity_summary_file_path))
        return rarity_summary


def extract_token_properties_from_metadata_csv(token_metadata_csv):
    df_raw_token_metadata = ensure_dataframe(token_metadata_csv)
    is_html = df_raw_token_metadata['properties'].astype(str).str.startswith('<!DOCTYPE html>').dropna()
    if len(is_html) == len(df_raw_token_metadata) and is_html.all():
        print('All metadata are HTML, skip extracting token properties')
        return None
    print(f'There are {df_raw_token_metadata["properties"].apply(type).eq(str).sum()} String-type metadata among total {len(df_raw_token_metadata)} metadata')
    df_raw_token_attributes = df_raw_token_metadata['properties']
    columns = set()
    # use mask to convert the df_raw_token_attributes to unified format
    # to be connivent for extracting columns
    # df_raw_token_attributes = df_raw_token_metadata['metadata'].mask(lambda x: ~isinstance(x, dict) or 'attributes' not in x).map(lambda x: [{'trait_type': key, 'value': value} if isinstance(x, dict) else x for key, value in x.get('attributes', {}).items()]).dropna()
    # df_raw_token_attributes = pd.DataFrame(df_raw_token_attributes.to_list())
    # columns = set(df_raw_token_attributes.columns)
    df_raw_token_attributes = df_raw_token_metadata['properties'].apply(lambda x: ast.literal_eval(x))
    for token_attributes in df_raw_token_attributes:
        if token_attributes is None or token_attributes == np.nan or type(token_attributes) in [int, float]:
            continue
        if isinstance(token_attributes, dict):
            if 'trait_type' in token_attributes:
                columns.add(token_attributes['trait_type'])
            else:
                columns |= set(token_attributes)
        else:
            for attribute in token_attributes:
                if attribute is None or 'trait_type' not in attribute:
                    continue
                columns.add(attribute['trait_type'])
    print(f'{len(columns)} columns: {columns}')

    if len(columns) == 0:
        print('No token properties')
        return None

    if len(columns) >= 500:
        print(f'Too many columns ({len(columns)}), skip extracting token properties')
        return None

    df_token_properties = pd.DataFrame(columns=['contract_address', 'token_id']+list(columns))
    df_token_properties['contract_address'] = df_raw_token_metadata['contract_address']
    df_token_properties['token_id'] = df_raw_token_metadata['token_id']
    for i, token_attributes in enumerate(df_raw_token_attributes):
        if token_attributes is None or token_attributes == np.nan or type(token_attributes) in [int, float]:
            continue
        if isinstance(token_attributes, dict):
            if 'trait_type' in token_attributes:
                df_token_properties.loc[i, token_attributes['trait_type']] = token_attributes.get('value', None)
            else:
                # print(token_attributes.keys(), token_attributes.values())
                df_token_properties.loc[i, token_attributes.keys()] = token_attributes.values()
        else:
            for attribute in token_attributes:
                if attribute is None or 'trait_type' not in attribute:
                    continue
                df_token_properties.loc[i, attribute['trait_type']] = attribute.get('value', None)
                # df_token_properties[attribute['trait_type']][i] = attribute['value']
    id_columns = ['contract_address', 'token_id']
    reindexed_columns = id_columns + sorted(list(columns))
    df_token_properties = df_token_properties.reindex(reindexed_columns, axis=1)
    assert len(df_token_properties) == len(df_raw_token_metadata)
    return df_token_properties



def summarize_dataset(dataset_dir):
    chain_save_dir = os.path.join(dataset_dir, chain)
    contract_address_list = os.listdir(chain_save_dir)
    contract_folder_list = [os.path.join(chain_save_dir, contract_address) for contract_address in contract_address_list]
    df_dataset_info = pd.DataFrame({'contract_address': contract_address_list, 'contract_folder': contract_folder_list})
    file_paths = {
        'token_metadata_file_path': 'token_metadata.json',
        'token_sales_file_path': 'token_sales.json',
        'token_properties_file_path': 'token_properties.csv',
        'rarity_summary_file_path': 'rarity_summary.json',
        'token_sales_page_key_file_path': 'token_sales_page_key.txt'
    }
    for key, value in file_paths.items():
        df_dataset_info[key] = df_dataset_info['contract_address'].apply(lambda x: os.path.join(chain_save_dir, x, value) if os.path.exists(os.path.join(chain_save_dir, x, value)) else None)

    df_contract_metadata = pd.read_csv(os.path.join(dataset_dir, 'contract_metadata.csv'))
    df_dataset_info = df_contract_metadata.merge(df_dataset_info, left_on='address', right_on='contract_address', how='left')
    df_dataset_info['downloaded'] = df_dataset_info['token_metadata_file_path'].apply(lambda x: True if x is not None else False)
    # df_dataset_info['num_tokens'] = df_dataset_info['token_metadata_file_path'].apply(lambda x: len(read_json(x)) if x is not None else None)
    # df_dataset_info['num_token_sales'] = df_dataset_info['token_sales_file_path'].apply(lambda x: len(read_json(x)) if x is not None else None)
    df_dataset_info.to_csv(os.path.join(dataset_dir, 'dataset_info.csv'), index=False, escapechar='\\')
    print(f'Total {len(df_dataset_info)} contracts, Downloaded {len(df_dataset_info[df_dataset_info["downloaded"]])} contracts')
    print('Saved to:', os.path.join(dataset_dir, 'dataset_info.csv'))

def scrape_one_contract(alchemy_data_scraper, contract_address, metadata=False, sales=True, transfers=True, rarity=True):
    print(f'=' * 100)
    if metadata:
        print(f'Scraping contract tokens metadata of {contract_address}')
        df_contract_tokens_metadata = alchemy_data_scraper.scrap_contract_token_metadata(contract_address)
    if sales:
        print(f'-' * 100)
        print(f'Scraping contract tokens sales of {contract_address}')
        df_contract_tokens_sales = alchemy_data_scraper.scrap_contract_token_sales(contract_address)
    if transfers:
        print(f'-' * 100)
        print(f'Scraping contract tokens transfers of {contract_address}')
        df_contract_tokens_transfers = alchemy_data_scraper.scrap_contract_token_transfers(contract_address)
    if rarity:
        print(f'-' * 100)
        print(f'Scraping contract rarity summary of {contract_address}')
        df_contract_rarity_summary = alchemy_data_scraper.scrap_contract_rarity_summary(contract_address)
    print(f'=' * 100)


def scrape_contract(alchemy_data_scraper, contract_address, metadata=False, sales=False, transfers=False, rarity=True):
    # if contract_address in invalid_contract_address: continue
    if contract_address in [
        '0x25ec84abe25174650220b83841e0cfb39d8aab87',
        '0x495f947276749ce646f68ac8c248420045cb7b5e', # too many transfers
        '0xe627938356cb7383b8819f2dd20114333e3842d2', # too many transfers
        '0xd07dc4262bcdbf85190c01c996b4c06a461d2430', # too many transfers
        '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85', # ENS
        ]: return None
    # special cases
    # 0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85: ENS
    # 0x626a67477d2dca67cac6d8133f0f9daddbfea94e: some metadata is string
    # 0x92c93fafc20fe882a448f86e594d9667259c42c8: only one attributes saving in dict type
    # 0xd31fc221d2b0e0321c43e9f6824b26ebfff01d7d
    print(f'Scraping contract {contract_address}')
    # df_contract_tokens_metadata = alchemy_data_scraper.scrap_contract_token_metadata(contract_address, sample=False)
    if metadata:
        print(f'Scraping contract tokens metadata of {contract_address}')
        df_contract_tokens_metadata = alchemy_data_scraper.scrap_contract_token_metadata(contract_address)
    if sales:
        df_contract_tokens_metadata = alchemy_data_scraper.scrap_contract_token_sales(contract_address)
    if transfers:
        df_contract_tokens_metadata = alchemy_data_scraper.scrap_contract_token_transfers(contract_address)
    if rarity:
        df_contract_rarity_summary = alchemy_data_scraper.scrap_contract_rarity_summary(contract_address)
    print(f'Complete contract {contract_address}')
    return 


if __name__ == '__main__':
    import os
    from multiprocessing import Pool
    from dotenv import load_dotenv

    load_dotenv()
    alchemy_dataset_dir = os.getenv('ALCHEMY_DATA_DIR')
    alchemy_api_key = os.getenv('ALCHEMY_API_KEY')
    chain = os.getenv('CHAIN')


    ### ----------------- Set the following parameters ----------------- ###
    num_top_collections = 1000
    num_processes = 4
    # Replace the top_collections_file_path with the actual path obatained from the previous step
    top_collections_file_path = 'data/shroomdk/top_50000_collections_info_2017-01-01_2024-07-23.csv'
    # The following parameters can be set to True to scrape the corresponding data
    metadata, sales, transfers, rarity = True, True, True, True
    ### ----------------------------------------------------------- ###

    alchemy_data_scraper = NftAlchemyScraper(alchemy_api_key, if_reuse=True, sleep_time=0.05, main_save_dir=alchemy_dataset_dir, chain=chain)

    pool = Pool(num_processes)
    results = []
    contract_address_list = pd.read_csv(top_collections_file_path)[:num_top_collections]['address'].to_list()
    for i, contract_address in enumerate(contract_address_list):
        results.append(pool.apply_async(scrape_contract, args=(alchemy_data_scraper, contract_address, metadata, sales, transfers, rarity, )))
    pool.close()
    pool.join()
    df_contract_tokens_metadata_list = [r.get() for r in results if r.get() is not None]


