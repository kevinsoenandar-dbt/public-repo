"""
refresh_aws_creds.py

Fetches a set of temporary AWS credentials via STS get_session_token and
pushes them into the dbt platform as user-level environment variable overrides.
The three variables updated are:
  - DBT_ENV_SECRET_AWS_ACCESS_KEY_ID
  - DBT_ENV_SECRET_AWS_SECRET_ACCESS_KEY
  - DBT_ENV_SECRET_AWS_SESSION_TOKEN

Configuration is read from environment variables or a local .env file.
See .env.example for required and optional settings.

Usage:
    python refresh_aws_creds.py

Requirements:
    pip install -r requirements.txt
"""

import os
import sys

import boto3
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DBT_BASE_URL = "https://cloud.getdbt.com"

TARGET_ENV_VARS = [
    "DBT_ENV_SECRET_AWS_ACCESS_KEY_ID",
    "DBT_ENV_SECRET_AWS_SECRET_ACCESS_KEY",
    "DBT_ENV_SECRET_AWS_SESSION_TOKEN",
]


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """Load required and optional config from environment (with .env fallback)."""
    load_dotenv()

    required = ["DBT_API_KEY", "DBT_ACCOUNT_ID", "DBT_PROJECT_ID", "DBT_USER_ID"]
    config = {}
    missing = []

    for key in required:
        value = os.environ.get(key)
        if not value:
            missing.append(key)
        config[key] = value

    if missing:
        print(f"[ERROR] Missing required environment variables: {', '.join(missing)}")
        print("Copy .env.example → .env and fill in your values.")
        sys.exit(1)

    # Optional STS / MFA settings
    config["AWS_MFA_SERIAL"] = os.environ.get("AWS_MFA_SERIAL")
    config["AWS_MFA_TOKEN"] = os.environ.get("AWS_MFA_TOKEN")
    config["STS_DURATION_SECONDS"] = os.environ.get("STS_DURATION_SECONDS")

    return config


# ---------------------------------------------------------------------------
# Step 1: Get temporary AWS credentials via STS
# ---------------------------------------------------------------------------

def get_temporary_credentials(config: dict) -> dict:
    """
    Call STS get_session_token and return the Credentials dict.
    Supports optional MFA and custom duration.

    Returns a dict with keys: AccessKeyId, SecretAccessKey, SessionToken, Expiration.
    """
    sts_client = boto3.client("sts")

    kwargs = {}

    if config.get("STS_DURATION_SECONDS"):
        kwargs["DurationSeconds"] = int(config["STS_DURATION_SECONDS"])

    if config.get("AWS_MFA_SERIAL"):
        if not config.get("AWS_MFA_TOKEN"):
            print("[ERROR] AWS_MFA_SERIAL is set but AWS_MFA_TOKEN is missing.")
            sys.exit(1)
        kwargs["SerialNumber"] = config["AWS_MFA_SERIAL"]
        kwargs["TokenCode"] = config["AWS_MFA_TOKEN"]

    print("[INFO] Calling STS get_session_token ...")
    response = sts_client.get_session_token(**kwargs)
    credentials = response["Credentials"]

    print(f"[INFO] Temporary credentials obtained. Expire at: {credentials['Expiration']}")
    return credentials


# ---------------------------------------------------------------------------
# Step 2: List user-level env vars from the dbt platform
# ---------------------------------------------------------------------------

def list_user_env_vars(config: dict, session: requests.Session) -> list:
    """
    GET /api/v3/accounts/{account_id}/projects/{project_id}/environment-variables/user/
    Returns the full list of user-level environment variable objects.
    """
    url = (
        f"{DBT_BASE_URL}/api/v3/accounts/{config['DBT_ACCOUNT_ID']}"
        f"/projects/{config['DBT_PROJECT_ID']}/environment-variables/user/"
    )
    params = {
        "user_id": config["DBT_USER_ID"],
        "limit": 100,
        "offset": 0,
    }

    print("[INFO] Listing user-level environment variables from dbt ...")
    response = session.get(url, params=params)

    if not response.ok:
        print(f"[ERROR] Failed to list env vars: {response.status_code} {response.text}")
        sys.exit(1)

    data = response.json()
    # The API returns an enveloped response: { "data": [...], "status": {...} }
    env_vars = data.get("data", [])
    print(f"[INFO] Found {len(env_vars)} environment variable(s) for the user.")
    return env_vars


# ---------------------------------------------------------------------------
# Step 3: Update each credential variable
# ---------------------------------------------------------------------------

def update_env_var(
    config: dict,
    session: requests.Session,
    env_var_id: int,
    name: str,
    value: str,
) -> None:
    """
    POST /api/v3/accounts/{account_id}/projects/{project_id}/environment-variables/{id}/
    Updates a single environment variable with a new raw value.
    """
    url = (
        f"{DBT_BASE_URL}/api/v3/accounts/{config['DBT_ACCOUNT_ID']}"
        f"/projects/{config['DBT_PROJECT_ID']}/environment-variables/{env_var_id}/"
    )
    payload = {
        "id": env_var_id,
        "account_id": int(config["DBT_ACCOUNT_ID"]),
        "project_id": int(config["DBT_PROJECT_ID"]),
        "user_id": int(config["DBT_USER_ID"]),
        "name": name,
        "type": "user",
        "raw_value": value,
    }

    response = session.post(url, json=payload)

    if not response.ok:
        print(f"[ERROR] Failed to update {name}: {response.status_code} {response.text}")
        sys.exit(1)

    print(f"[INFO]   ✅ Updated {name}")


def create_env_var(
    config: dict,
    session: requests.Session,
    name: str,
    value: str,
) -> None:
    """
    POST /api/v3/accounts/{account_id}/projects/{project_id}/environment-variables/
    Creates a new user-level environment variable override.
    """
    url = (
        f"{DBT_BASE_URL}/api/v3/accounts/{config['DBT_ACCOUNT_ID']}"
        f"/projects/{config['DBT_PROJECT_ID']}/environment-variables/"
    )
    payload = {
        "account_id": int(config["DBT_ACCOUNT_ID"]),
        "project_id": int(config["DBT_PROJECT_ID"]),
        "user_id": int(config["DBT_USER_ID"]),
        "name": name,
        "type": "user",
        "raw_value": value,
    }

    response = session.post(url, json=payload)

    if not response.ok:
        print(f"[ERROR] Failed to create {name}: {response.status_code} {response.text}")
        sys.exit(1)

    print(f"[INFO]   ✅ Created {name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = load_config()

    # --- Step 1: Fetch temporary AWS credentials ---
    aws_creds = get_temporary_credentials(config)

    credential_map = {
        "DBT_ENV_SECRET_AWS_ACCESS_KEY_ID":     aws_creds["AccessKeyId"],
        "DBT_ENV_SECRET_AWS_SECRET_ACCESS_KEY": aws_creds["SecretAccessKey"],
        "DBT_ENV_SECRET_AWS_SESSION_TOKEN":     aws_creds["SessionToken"],
    }

    # --- Set up authenticated dbt API session ---
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {config['DBT_API_KEY']}",
        "Content-Type": "application/json",
    })

    # --- Step 2: List current user-level env vars to find IDs ---
    env_vars = list_user_env_vars(config, session)

    # Build a name → user-level id lookup (only where a user override already exists)
    name_to_user_id = {}
    for name, env_var in env_vars.items():
        if name in TARGET_ENV_VARS:
            user_override = env_var.get("user")
            if user_override and user_override.get("id"):
                name_to_user_id[name] = user_override["id"]

    # --- Step 3: Create or update each variable with the new credential value ---
    print("[INFO] Creating or Updating dbt environment variables ...")
    for name, value in credential_map.items():
        if name in name_to_user_id:
            update_env_var(config, session, name_to_user_id[name], name, value)
        else:
            create_env_var(config, session, name, value)

    print(
        f"\n[SUCCESS] All 3 AWS credential variables updated successfully.\n"
        f"          Credentials expire at: {aws_creds['Expiration']}"
    )


if __name__ == "__main__":
    main()
