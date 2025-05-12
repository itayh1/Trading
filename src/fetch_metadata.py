import financedatabase as fd


def main():
    # Load all equity data
    equities = fd.Equities()

    # Filter for U.S. stocks with market capitalization over $2 billion
    large_cap_stocks = equities.search(country='United States', market_cap='Large Cap', currency='USD')

    # Display the first few entries
    print(large_cap_stocks.head())


if __name__ == '__main__':
    main()