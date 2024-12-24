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

    if not url or not the_word:
        print("Usage: python trigger_idx_load.py <url> <word>")
        sys.exit(1)

    for year in range(2020, 2025):
        for qtr in range(1, 5):
            print(f"Trigger load for {year}/QTR{qtr}")
            status, data = trigger_idx_load(url, year, qtr, the_word)
            if status != 200:
                print(f"Status: {status}")
                print(f"Response: {data}")
