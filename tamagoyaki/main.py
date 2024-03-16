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
from rich import print

# const
WORKING_DIR = f"{Path.home()}/.tamagoyaki_db"

logger.remove()
logger.add(f"{WORKING_DIR}/logs/tamagoyaki.log", level="INFO", format="{time} {level} {message}")
app = typer.Typer()


@app.callback(help="A CLI tool for managing the crypto candlestick data.")
def callback() -> None:
    """ callback
    initialize the working directory.
    """

    print("This is tamagoyaki🍳")
    print("Can you hear me?")

    os.makedirs(WORKING_DIR, exist_ok=True)


@app.command(help="update the database.")
def update(
    symbol: str = typer.Argument(help="The symbol to download"),
    begin: str = typer.Argument(help="The begin date (YYYYMMDD)"),
    end: str = typer.Argument(help="The end date (YYYYMMDD)"),
    interval_sec: int = typer.Argument(60, help="The interval of the candlestick in seconds. (default: 60)"),
) -> None:
    """ update
    
    """
    
    # validate the date format
    try:
        bdt = datetime.datetime.strptime(begin, "%Y%m%d")
        edt = datetime.datetime.strptime(end, "%Y%m%d")
    except ValueError:
        err = "Invalid date format. Please use YYYYMMDD."
        raise typer.BadParameter(err)
    
    # main process
    db = Database(f"sqlite:///{WORKING_DIR}/{symbol}.db")

    date_range = [bdt + datetime.timedelta(days=i) for i in range((edt - bdt).days + 1)]
    for date in date_range:

        print("DEBUG: download -> {}".format(date.strftime("%Y-%m-%d")))
        
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
            reader = csv.reader(f)
            next(reader)

            pdt = None
            op, hi, lo, cl, vol, bvol, svol = .0, .0, .0, .0, .0, .0, .0
            for row in reader:

                dt = datetime.datetime.fromtimestamp(float(row[0]), datetime.timezone.utc)
                truncated_dt = dt - datetime.timedelta(seconds=dt.second % interval_sec, microseconds=dt.microsecond)

                # update the variables
                if truncated_dt != pdt:

                    candles.append(Candle(
                        dt=truncated_dt,
                        open=op,
                        high=hi,
                        low=lo,
                        close=cl,
                        volume=vol,
                        buy_volume=bvol,
                        sell_volume=svol
                    ))

                    pdt = truncated_dt
                    op = float(row[4])
                    hi = float(row[4])
                    lo = float(row[4])
                    cl = float(row[4])
                    vol = float(row[3])
                    bvol = float(row[3]) if row[2] == "Buy" else .0
                    svol = float(row[3]) if row[2] == "Sell" else .0

                    continue
                
                hi = max(hi, float(row[4]))
                lo = min(lo, float(row[4]))
                cl = float(row[4])
                vol += float(row[3])
                bvol += float(row[3]) if row[2] == "Buy" else .0
                svol += float(row[3]) if row[2] == "Sell" else .0

            candles.append(Candle(
                dt=pdt,
                open=op,
                high=hi,
                low=lo,
                close=cl,
                volume=vol,
                buy_volume=bvol,
                sell_volume=svol
            ))
        
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
