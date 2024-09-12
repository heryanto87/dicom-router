import requests
import os

# Load configuration
whatsapp_provider = os.getenv('WHATSAPP_PROVIDER')
halosis_email = os.getenv('HALOSIS_EMAIL')
halosis_pass = os.getenv('HALOSIS_PASS')

def get_token():
    """Fetches long-lived token by logging in and requesting a refresh token."""

    login_url = f'{whatsapp_provider}/v1/login'
    access_token_url = f'{whatsapp_provider}/v1/access-token'

    # Prepare payload for login request
    login_payload = {
        'email': halosis_email,
        'password': halosis_pass
    }

    try:
        # Perform login request
        login_response = requests.post(login_url, json=login_payload)
        login_response.raise_for_status()  # Raises an exception for 4XX/5XX status codes

        response_data = login_response.json()
        refresh_token = response_data.get('refresh_token')

        if not refresh_token:
            raise ValueError("No refresh token found in the login response.")

        # Prepare payload for access token request
        token_payload = {'refresh_token': refresh_token}

        # Request long-lived token
        token_response = requests.post(access_token_url, json=token_payload)
        token_response.raise_for_status()  # Raises an exception for 4XX/5XX status codes

        token_data = token_response.json()
        return {
            "token": token_data.get('long_lived_token'),
            "expire_at": token_data.get('token_expired_at'),
        }

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except ValueError as val_err:
        print(f"Value error: {val_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

    return None
