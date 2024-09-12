import logging
import requests
import os

# Set up logger
LOGGER = logging.getLogger('pynetdicom')
logging.basicConfig(level=logging.INFO)

# Load configuration
url = os.getenv('URL')
client_key = os.getenv('CLIENT_KEY')
secret_key = os.getenv('SECRET_KEY')


def get_token():
    """
    Retrieves an OAuth2 token using client credentials.

    Returns:
        str: The access token, or None if the request fails.
    """
    payload = {
        'client_id': client_key,
        'client_secret': secret_key
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'PostmanRuntime/7.26.8',
    }

    token_url = f"{url}/oauth2/v1/accesstoken?grant_type=client_credentials"

    try:
        # Request token
        LOGGER.info("Requesting OAuth2 token from %s", token_url)
        response = requests.post(token_url, data=payload, headers=headers, verify=False)

        # Log response headers and request details for debugging
        LOGGER.debug("Response headers: %s", response.headers)
        LOGGER.debug("Request headers: %s", response.request.headers)
        LOGGER.debug("Payload: %s", payload)

        # Check for HTTP errors
        response.raise_for_status()

        data = response.json()
        LOGGER.info("OAuth2 token retrieved successfully.")

        token = data.get("access_token")
        if token:
            LOGGER.info("Access token: %s", token)
            return token
        else:
            LOGGER.error("No access token found in the response.")
            return None

    except requests.exceptions.RequestException as e:
        LOGGER.error("Failed to request OAuth2 token: %s", e)
        return None
