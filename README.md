# Simple Example

To add 60-second candlestick data to the BTCUSDT database for January 1-3, 2024
```
$ python3 tamagoyaki/main.py update BTCUSDT 20240101 20240103
```

Then save to csv file in the current directory.
```
$ python3 tamagoyaki/main.py generate BTCUSDT 20240101 20240103 60
``` 