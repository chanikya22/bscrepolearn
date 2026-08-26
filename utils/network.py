import requests

def post_request(
    url,
    headers=None,
    params=None,
    data=None,
    json=None,
    timeout=30,
    verify_ssl=True
):
    """
    Makes a POST request to the given URL with optional headers, parameters, and data.

    :param url: The endpoint URL to make the POST request.
    :param headers: (Optional) Dictionary of HTTP headers.
    :param params: (Optional) Dictionary of query parameters.
    :param data: (Optional) Dictionary or string of form-encoded data to send in the body.
    :param json: (Optional) Dictionary of JSON data to send in the body.
    :param timeout: (Optional) Timeout for the request in seconds (default is 30).
    :param verify_ssl: (Optional) Verify SSL certificates (default is True).
    :return: Response object from the request.
    """
    try:
        response = requests.post(
            url,
            headers=headers,
            params=params,
            data=data,
            json=json,
            timeout=timeout,
            verify=verify_ssl
        )
        response.raise_for_status()  # Raise an error for HTTP status codes 4xx/5xx
        return response
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the POST request: {e}")
        return None