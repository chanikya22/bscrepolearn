import cloudscraper
import json
import time
import random


def make_blinkit_request():
    # Create cloudscraper session with stealth options
    scraper = cloudscraper.create_scraper(
        interpreter='js2py',
        delay=10,  # Delay for challenge solving
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )

    # API endpoint
    url = "https://blinkit.com/v1/layout/listing_widgets"

    # Query parameters
    params = {
        'offset': 15,
        'limit': 15,
        'exclude_combos': 'false',
        'l0_cat': 332,
        'l1_cat': 1102,
        'last_snippet_type': 'product_card_snippet_type_2',
        'last_widget_type': 'product_container',
        'oos_visibility': 'true',
        'page_index': 1,
        'total_entities_processed': 1,
        'total_pagination_items': 160
    }

    # Headers from your curl command
    headers = {
        'x-age-consent-granted': 'true',
        'sec-ch-ua-platform': '"Windows"',
        'lat': '28.4489039',
        'session_uuid': 'bd58635b-dfc9-4ef7-8f24-afd8a629f366',
        'web_app_version': '1008010016',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'app_client': 'consumer_web',
        'device_id': 'dde82fe0-8473-498e-86e1-3550f2c1b9d4',
        'auth_key': 'c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477',
        'Content-Type': 'application/json',
        'lon': '77.0833742',
        'platform': 'desktop_web',
        'Referer': 'https://blinkit.com/cn/soft-drinks/cid/332/1102',
        'app_version': '1010101010',
        'rn_bundle_version': '1009003012',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'access_token': 'null'
    }

    # Request payload
    payload = {
        "applied_filters": None,
        "is_sr_rail_visible": False,
        "is_subsequent_page": False,
        "postback_meta": {
            "primary_results_group_ids": [2163461, 1951334, 1090823, 1951721, 1951406, 1951502, 1951919, 1951888,
                                          1937011, 2152692, 2152696, 1951483, 1951485, 1951166, 1318527],
            "primary_results_product_ids": [519552, 532874, 9483, 532876, 532875, 17678, 280, 436777, 484783, 331827,
                                            307, 331830, 421687, 312, 15288, 562364, 562371, 396484, 589507, 396486,
                                            554691, 396488, 396489, 589514, 520019, 522837, 555222, 598361, 492255,
                                            492258, 225784, 539116, 536179, 536183, 536184, 86521, 482427, 536190]
        },
        "processed_product_ids": None,
        "processed_rails": {
            "aspirational_card_rail": {
                "total_count": 0,
                "processed_count": 5,
                "processed_product_ids": []
            },
            "attribute_rail": {
                "total_count": 0,
                "processed_count": 4,
                "processed_product_ids": []
            },
            "brand_rail": {
                "total_count": 0,
                "processed_count": 1,
                "processed_product_ids": []
            },
            "dc_rail": {
                "total_count": 0,
                "processed_count": 1,
                "processed_product_ids": []
            },
            "priority_dc_rail": {
                "total_count": 0,
                "processed_count": 1,
                "processed_product_ids": []
            }
        },
        "product_ids": None,
        "shown_product_count": 15,
        "sort": ""
    }

    try:
        print("Making request to Blinkit API...")

        # Add random delay to appear more human-like
        time.sleep(random.uniform(1, 3))

        # Make the POST request
        response = scraper.post(
            url=url,
            params=params,
            headers=headers,
            json=payload,
            timeout=30
        )

        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")

        if response.status_code == 200:
            try:
                data = response.json()
                print("\n=== API Response ===")
                print(json.dumps(data, indent=2))
                return data
            except json.JSONDecodeError:
                print("Response is not valid JSON")
                print(f"Raw response: {response.text[:1000]}...")
                return None
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response content: {response.text[:1000]}...")
            return None

    except cloudscraper.exceptions.CloudflareChallengeError as e:
        print(f"Cloudflare challenge error: {e}")
        return None
    except Exception as e:
        print(f"Request error: {e}")
        return None


def make_multiple_requests(count=1, delay_range=(5, 10)):
    """Make multiple requests with delays"""
    results = []

    for i in range(count):
        print(f"\n--- Request {i + 1}/{count} ---")
        result = make_blinkit_request()
        results.append(result)

        if i < count - 1:  # Don't delay after the last request
            delay = random.uniform(delay_range[0], delay_range[1])
            print(f"Waiting {delay:.1f} seconds before next request...")
            time.sleep(delay)

    return results


if __name__ == "__main__":
    # Install required packages if not already installed
    try:
        import cloudscraper
    except ImportError:
        print("Please install cloudscraper: pip install cloudscraper")
        exit(1)

    # Make a single request
    print("=== Single Request ===")
    result = make_multiple_requests(count=1000)

    #Store this result in a text file
    with open('blinkit_response.json', 'w') as f:
        json.dump(result[0], f, indent=2)

    if result:
        print("\n✅ Request successful!")
    else:
        print("\n❌ Request failed!")

    # Uncomment below to make multiple requests
    # print("\n=== Multiple Requests ===")
    # results = make_multiple_requests(count=3, delay_range=(10, 20))
    # print(f"Completed {len([r for r in results if r])} successful requests out of {len(results)}")