import csv
import datetime
import gzip
import os
from io import BytesIO
from pathlib import Path

import requests
import typer
from database import Candle, Database
from loguru import logger
import numpy as np
import pandas as pd

# const
WORKING_DIR = f"{Path.home()}/.tamagoyaki"

logger.remove()
logger.add(f"{WORKING_DIR}/logs/tamagoyaki.log", level="INFO", format="{time} {level} {message}")
app = typer.Typer()


@app.callback(help="🍳 A CLI tool for managing the crypto candlestick data.")
def callback() -> None:
    """ callback
    initialize the working directory.
    """

    os.makedirs(WORKING_DIR, exist_ok=True)


@app.command(help="update the database.")
def update(
    symbol: str = typer.Argument(help="The symbol to download"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
) -> None:
    """ update

    update the database of 1-second candlestick data.

    """
    
    # validate the date format
    try:
        bdt = datetime.datetime.strptime(begin, "%Y%m%d")
        edt = datetime.datetime.strptime(end, "%Y%m%d")
    except ValueError:
        err = "Invalid date format. Please use YYYYMMDD."
        raise typer.BadParameter(err)
    
    # main process
    db = Database(f"sqlite:///{WORKING_DIR}/candle.db")

    date_range = [bdt + datetime.timedelta(days=i) for i in range((edt - bdt).days + 1)]

    for date in date_range:
        logger.info(f"Downloading {symbol}{date.strftime('%Y-%m-%d')}")

        # check if the data already exists
        query = db.session.query(Candle)
        query = query.filter(Candle.exchange == "bybit")
        query = query.filter(Candle.symbol == symbol)

        if query.filter(Candle.datetime == date).count() > 0:
            logger.info(f"{symbol}{date.strftime('%Y-%m-%d')} already exists.")
            continue
        
        # make url
        base_url = "https://public.bybit.com/trading/"
        filename = f"{symbol}{date.strftime('%Y-%m-%d')}.csv.gz"
        url = os.path.join(base_url, symbol, filename)

        # download
        resp = requests.get(url)
        if resp.status_code != 200:
            logger.error(f"Failed to download {filename}")
            continue
        
        # data processing
        candles: list[Candle] = []
        with gzip.open(BytesIO(resp.content), "rt") as f:

            df = pd.read_csv(f)

            # setting
            df = df[["timestamp", "side", "size", "price"]]
            df.loc[:, ["datetime"]] = pd.to_datetime(df["timestamp"], unit="s")
            df.loc[:, ["buySize"]] = np.where(df["side"] == "Buy", df["size"], 0)
            df.loc[:, ["sellSize"]] = np.where(df["side"] == "Sell", df["size"], 0)
            df.loc[:, ["datetime"]] = df["datetime"].dt.floor("1s")

            # groupby 
            df = df.groupby("datetime").agg(
                {
                    "price": ["first", "max", "min", "last"],
                    "size": "sum",
                    "buySize": "sum",
                    "sellSize": "sum",
                }
            )

            # multiindex to single index
            df.columns = ["_".join(col) for col in df.columns]
            df = df.rename(
                columns={
                    "price_first": "open",
                    "price_max": "high",
                    "price_min": "low",
                    "price_last": "close",
                    "size_sum": "volume",
                    "buySize_sum": "buyVolume",
                    "sellSize_sum": "sellVolume",
                }
            )

            # make Candle object
            for index, row in df.iterrows():
                candle = Candle(
                    exchange="bybit",
                    symbol=symbol,
                    datetime=index,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    buy_volume=row["buyVolume"],
                    sell_volume=row["sellVolume"],
                )
                candles.append(candle)
        
        # insert
        db.session.add_all(candles)
        db.session.commit()


@app.command()
def generate() -> None:
    """ generate

    """

    pass


if __name__ == "__main__":
    app()
