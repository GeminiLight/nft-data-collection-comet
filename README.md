# Light-weight NFT Data Collection Pipeline of COMET

Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Pipeline](#pipeline)
- [COMET Paper](#comet-paper)

## Introduction

In this repository, we provide a light-weight NFT data collection pipeline of COMET, including the top NFT collections, NFT token and transaction data, NFT file data, and coin price data.

This pipeline is built with several public APIs, which is an open-source alternative instead of laboriously building a blockchain node. We mainly use the [Alchemy](https://www.alchemy.com/), [Flipside](https://docs.flipsidecrypto.xyz/), and [CoinGecko](https://docs.coingecko.com/reference/introduction), which are widely used in the blockchain and cryptocurrency community. 

## Installation

```bash
pip install -r requirements.txt
```

## Pipeline

### API-Key Configuration

We can easily obtain the API keys by following steps: (a)Create an account on Alchemy or Shroomdk and (b) Get the API key from the dashboard. Then, we need to set the API keys in the file `.env`.

```bash
ALCHEMY_API_KEY=your_alchemy_api_key
SHROOMDK_API_KEY=your_shroomdk_api_key
```

Usually, the free tier of Alchemy and Shroomdk is enough for our data collection tasks.

### Data Collection

We provide a comprehensive data collection pipeline for NFT data, including the top NFT collections, NFT token and transaction data, NFT file data, and coin price data.

| Step | Description                                        | Source        | Script                              | Storage Directory                   |
|------|----------------------------------------------------|---------------|-------------------------------------|-------------------------------------|
| 1    | Scrape the top NFT collection information          | Shroomdk API  | `nft_shroomdk_scraper.py`           | `data/shroomdk`                     |
| 2    | Scrape token and transaction data of top NFTs      | Alchemy API   | `nft_alchemy_scraper.py`            | `data/coin_data`                    |
| 3    | Scrape file data of each top NFT collection        | Alchemy API   | `nft_file_scraper.py`               | `data/alchemy_nft_data/ETHEREUM/*/files`    |
| 4    | Scrape the price data of ETH and other coins       | CoinGecko API | `coin_price_scraper.py`             | `data/coin_data`                    |

1. Scrap the top NFT collection information

We first scrape the top NFT collections from the Shroomdk API.

```bash
python ./data_collection/nft_shroomdk_scraper.py
```

The scraped data is stored in the `data/shroomdk` directory.


2. Scrap the token and transaction data of each top NFT collection

We then scrape the NFT data of each top NFT collection from the Alchemy API.

```bash
python ./data_collection/nft_alchemy_scraper.py
```

The scraped data is stored in the `data/alchemy_nft_data/ETHEREUM` directory.


3. Scrap the file data of each top NFT

We scrape the file data of each top NFT collection from the Alchemy API.

```bash
python ./data_collection/nft_file_crawler.py
```

The scraped data is stored in the `data/alchemy_nft_data/ETHEREUM/*/files` directory.

4. Scrap the price data of ETH and other coins

We scrape the price data of ETH and other coins from the CoinGecko API.

```bash
python ./data_collection/coin_price_scraper.py
```

The scraped data is stored in the `data/coin_data` directory.


## COMET Paper

COMET is a comprehensive system for NFT price prediction with wallet profiling, offering an integrative solution for NFT data collection, analysis, and prediction. This system is detailed in our KDD-2024 ADS track paper titled "[COMET: NFT Price Prediction with Wallet Profiling](https://arxiv.org/abs/2405.10640)".

Due to corporate knowledge rights, we cannot release the full source code of COMET. However, we provide a lightweight NFT data collection pipeline from COMET for research purposes.

If you find this repository helpful, please consider citing our paper.

```bib
@inproceedings{wang2024comet,
    title={COMET: NFT Price Prediction with Wallet Profiling},
    author={Wang, Tianfu and Deng, Liwei and Wang, Chao and Lian, Jianxun and Yan, Yue and Yuan, Nicholas Jing and Zhang, Qi and Xiong, Hui},
    booktitle={Proceedings of the 30th ACM SIGKDD Conference on Knowledge Discovery and Data Mining},
    year={2024},
}
```