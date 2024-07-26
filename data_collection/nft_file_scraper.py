import os
import httpx
import pandas as pd
from PIL import Image
import filetype
import concurrent.futures
Image.MAX_IMAGE_PIXELS = None


supported_mime_types = {
    'png': 'image/png',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'webm': 'video/webm',
    'webp': 'image/webp',
    'apng': 'image/apng',
    'gif': 'image/gif',
    'svg': 'image/svg+xml',
    'mp4': 'video/mp4'
}

supported_formats = list(supported_mime_types.keys())



class NftFileScraper:

    def __init__(
            self, 
            save_dir, 
            chain='ethereum',
            verbose=True,
        ):
        self.save_dir = save_dir
        self.chain_dir = os.path.join(self.save_dir, chain)
        self.verbose = verbose
        os.makedirs(self.chain_dir, exist_ok=True)

    def ready_for_collection(self, collection_address):
        collection_address = collection_address.lower()
        self.collection_file_dir = os.path.join(self.chain_dir, collection_address, 'files')
        os.makedirs(self.collection_file_dir, exist_ok=True)
        return collection_address

    def download_token_image(self, collection_address, token_id, image_url, file_format=None):
        token_id = str(token_id)
        collection_address = self.ready_for_collection(collection_address)
        collection_file_dir = os.path.join(self.chain_dir, collection_address, 'files')
        download_token_ids = [nft_file_name.split('.')[0] for nft_file_name in os.listdir(collection_file_dir)]
        if token_id in download_token_ids:
            print(f'{token_id} already downloaded') if self.verbose else None
            return
        try:
            response = httpx.get(image_url)
            if response.status_code == 200:
                file_content = response.content
                if file_format == None:
                    kind = filetype.guess(file_content)
                    file_format = kind.extension if kind is not None else 'unknown'
                image_path = os.path.join(collection_file_dir, f'{token_id}.{file_format}')
                image_path = image_path.replace('.svg+xml', '.svg')
                with open(image_path, 'wb') as f:
                    f.write(file_content)
        except Exception as e:
            print(e)

    def download_collection_images(self, collection_address, df_token_metadata, max_num_files=10):
        preprocess_nft_token_metadata(df_token_metadata)
        print(f'Start downloading {collection_address}') if self.verbose else None
        self.ready_for_collection(collection_address)
        download_image_tasks = [(collection_address, token_id, image_url, file_format) 
                            for token_id, image_url, file_format in 
                            df_token_metadata[['token_id', 'file_url', 'file_format']].iloc[:max_num_files].values]
        for task in download_image_tasks:
            crawler.download_token_image(*task)
        print(f'Finish downloading {collection_address}') if self.verbose else None


def preprocess_nft_token_metadata(df_nft_token_metadata):
    assert 'file_url' in df_nft_token_metadata.columns, 'file_url is not in df_nft_file_info'
    assert 'file_format' in df_nft_token_metadata.columns, 'file_format is not in df_nft_file_info'
    if df_nft_token_metadata.empty or df_nft_token_metadata['file_url'].isnull().all():
        print(f'NFT file info is empty')
        return None
    df_nft_token_metadata = df_nft_token_metadata.dropna(subset=['file_url'])
    df_token_metadata = df_nft_token_metadata[~df_nft_token_metadata['file_url'].str.contains('ipfs')]
    df_token_metadata = df_token_metadata[df_token_metadata['file_url'].str.contains('https') | df_token_metadata['file_url'].str.contains('http')]
    df_token_metadata['file_format'] = df_token_metadata['file_format'].astype(str).str.lower()
    is_svg = df_token_metadata['file_format'].str.contains('svg')
    is_png = df_token_metadata['file_format'].str.contains('png')
    df_token_metadata.loc[is_svg, 'file_format'] = 'svg'
    df_token_metadata.loc[is_png, 'file_format'] = 'png'
    df_token_metadata = df_token_metadata[df_token_metadata['file_format'].isin(supported_formats)]
    if len(df_token_metadata) == 0:
        return None
    return df_token_metadata


def download_collection_list_images(collection_address_list):
    i = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_collection_images, collection_address) 
                   for collection_address in collection_address_list
                   if os.path.isdir(os.path.join(data_dir, collection_address))]
        for future in concurrent.futures.as_completed(futures):
            # try:
            future.result()
            i += 1
            print(f'Finish downloading {i} collections')


if __name__ == '__main__':
    import dotenv
    dotenv.load_dotenv()
    alchemy_chain_dir = os.getenv('ALCHEMY_CHAIN_DIR')
    nft_file_dir = os.getenv('NFT_FILE_DIR')

    crawler = NftFileScraper(nft_file_dir)
    images_to_download = []


    max_num_files = 20
    max_workers = 1
    all_collection_address_list = os.listdir(alchemy_chain_dir)[2800:]

    address = all_collection_address_list[0]
    token_metadata_sample_file_path = os.path.join(alchemy_chain_dir, address, 'token_metadata_sample.csv')
    df = pd.read_csv(token_metadata_sample_file_path)
    df = df.rename(columns={'media_url': 'file_url', 'media_format': 'file_format'})[['token_id', 'file_url', 'file_format']]
    # df = preprocess_nft_token_metadata(df)
    crawler.download_collection_images(address, df, max_num_files=max_num_files)