import http.client
import sys
import urllib.parse


def trigger_idx_load(url, year, qtr, the_word):
    """Send an HTTP request with query parameters year, qtr, and word."""
    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.urlencode({"year": year, "qtr": qtr, "word": the_word})
    full_path = f"{parsed_url.path}?{query_params}"

    connection = http.client.HTTPSConnection(parsed_url.netloc)
    connection.request("GET", full_path)
    response = connection.getresponse()
    data = response.read()
    connection.close()

    return response.status, data.decode()


if __name__ == "__main__":
    url = sys.argv[1]
    the_word = sys.argv[2]

    begin_year = int(sys.argv[3]) if len(sys.argv) > 3 else 2023
    end_year = int(sys.argv[4]) if len(sys.argv) > 4 else 2024

    if not url or not the_word:
        print("Usage: python trigger_idx_load.py <url> <word> <begin_year> <end_year>")
        sys.exit(1)

    for year in range(begin_year, end_year):
        for qtr in range(1, 5):
            print(f"Trigger load for {year}/QTR{qtr}")
            status, data = trigger_idx_load(url, year, qtr, the_word)
            if status != 200:
                print(f"Status: {status}")
                print(f"Response: {data}")
