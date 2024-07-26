import os
import pandas as pd

from pycoingecko import CoinGeckoAPI


class CoinPriceScraper:
    """
    Collect the coin price data from CoinGecko
    """
    def __init__(
            self, 
            save_dir='data/coin_data', 
            if_save=False,
            verbose=True):
        self.cg = CoinGeckoAPI()
        self.if_save = if_save
        self.save_dir = save_dir
        self.verbose = verbose
        self.prices_save_dir = f'{self.save_dir}/prices'
        if self.if_save:
            os.makedirs(self.save_dir, exist_ok=True)
            os.makedirs(self.prices_save_dir, exist_ok=True)

    def scrap_all_coins(self):
        coins_list = self.cg.get_coins_list()
        df_coins = pd.DataFrame(coins_list)
        print(f'Get {len(df_coins)} coins') if self.verbose else None
        if self.if_save:
            coins_file_path = f'{self.save_dir}/coins.csv'
            df_coins.to_csv(coins_file_path, index=False)
            print(f'Save the coin list to {coins_file_path}') if self.verbose else None
        return df_coins

    def scrap_all_coins_markets(self, target_coin='usd'):
        coins_markets_list = self.cg.get_coins_markets(target_coin)
        df_coins_markets = pd.DataFrame(coins_markets_list)
        print(f'Get {len(df_coins_markets)} coins markets') if self.verbose else None
        if self.if_save:
            coins_markets_file_path = f'{self.save_dir}/coins_markets.csv'
            df_coins_markets.to_csv(coins_markets_file_path, index=False)
            print(f'Save the coin markets to {coins_markets_file_path}') if self.verbose else None
        return df_coins_markets

    def scrap_coin_daily_prices(self, source_coin, target_coin='usd', days='max', interval='daily'):
        coin_daily_prices_file_path = f'{self.prices_save_dir}/{source_coin}_{target_coin}_daily_prices.csv'
        coin_daily_prices = self.cg.get_coin_market_chart_by_id(source_coin, vs_currency=target_coin, days=days, interval=interval)
        df_coin_daily_prices = pd.DataFrame(coin_daily_prices)
        df_coin_daily_prices['timestamp'] = df_coin_daily_prices['prices'].apply(lambda x: x[0])
        df_coin_daily_prices['date'] = pd.to_datetime(df_coin_daily_prices['timestamp'], utc=True, unit='ms')
        df_coin_daily_prices['prices'] = df_coin_daily_prices['prices'].apply(lambda x: x[1])
        df_coin_daily_prices['market_caps'] = df_coin_daily_prices['market_caps'].apply(lambda x: x[1])
        df_coin_daily_prices['total_volumes'] = df_coin_daily_prices['total_volumes'].apply(lambda x: x[1])
        df_coin_daily_prices['source_coin'] = source_coin
        df_coin_daily_prices['target_coin'] = target_coin

        oldest_date = df_coin_daily_prices['date'].min()
        newest_date = df_coin_daily_prices['date'].max()
        print(f'Get the {source_coin} -> {target_coin} daily price of {source_coin} from {oldest_date} to {newest_date}') if self.verbose else None
        if self.if_save:
            df_coin_daily_prices.to_csv(coin_daily_prices_file_path, index=False)
            print(f'Save the coin daily price to {coin_daily_prices_file_path}') if self.verbose else None
        return df_coin_daily_prices


if __name__ == '__main__':
    import dotenv

    dotenv.load_dotenv()
    coin_save_dir = os.getenv('COIN_SAVE_DIR')

    coin_price_scraper = CoinPriceScraper(
        save_dir=coin_save_dir,
        if_save=True,
        verbose=True
    )
    df_coins = coin_price_scraper.scrap_all_coins()
    df_coins_markets = coin_price_scraper.scrap_all_coins_markets()
    df_coin_daily_prices = coin_price_scraper.scrap_coin_daily_prices('ethereum')