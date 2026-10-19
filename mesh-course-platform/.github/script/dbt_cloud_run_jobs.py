import enum
import os
import time
import pendulum

import requests

# capture environment variables from the bitbucket-pipelines configuration yml
ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID")
PROJECT_ID = os.getenv("DBT_CLOUD_PROJECT_ID")
PARSE_JOB_ID = os.getenv("DBT_CLOUD_JOB_ID_PARSE")
BUILD_JOB_ID = os.getenv("DBT_CLOUD_JOB_ID_BUILD")
API_KEY = os.getenv("DBT_CLOUD_API_TOKEN")
COMMIT_ID = os.getenv("GITHUB_SHA")[:7]

# define a class of different dbt Cloud API status responses in integer format
class DbtJobRunStatus(enum.IntEnum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


# trigger the dbt Cloud job
def _trigger_job(job_run_id: int) -> int:
    res = requests.post(
        url=f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{job_run_id}/run/",
        headers={"Authorization": f"Token {API_KEY}"},
        data={
            "cause": f"Github Merge",
            "git_sha": f"{COMMIT_ID}",
        },
    )

    try:
        res.raise_for_status()
    except:
        print(f"API token (last four): ...{API_KEY[-4:]}")
        raise

    response_payload = res.json()
    return response_payload["data"]["id"]


# to be used in a while loop to check on job status
def _get_job_run_status(job_run_id):
    res = requests.get(
        url=f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/runs/{job_run_id}/",
        headers={"Authorization": f"Token {API_KEY}"},
    )

    res.raise_for_status()
    response_payload = res.json()
    return response_payload["data"]["status"]


# main function operator to trigger the job and a while loop to wait for success or error
def run():
    parse_job_run_id = _trigger_job(PARSE_JOB_ID)
    build_job_run_id = _trigger_job(BUILD_JOB_ID)

    print(f"parse_job_run_id = {parse_job_run_id} & build_job_run_id = {build_job_run_id}")
    visit_url = f"https://cloud.getdbt.com/#/accounts/{ACCOUNT_ID}/projects/{PROJECT_ID}/runs/{build_job_run_id}/"

    timeout = 30
    end_time = pendulum.now().add(minutes=timeout)

    while pendulum.now() < end_time:
        time.sleep(5)

        status = _get_job_run_status(build_job_run_id)

        print(f"status = {status}")

        if status == DbtJobRunStatus.SUCCESS:
            print(f"Success! Visit URL: {visit_url}")
            break
        elif status == DbtJobRunStatus.ERROR or status == DbtJobRunStatus.CANCELLED:
            raise Exception(f"Failure! Visit URL: {visit_url}")
    
    else:  # This executes if the while loop completes without breaking (timeout case)
        raise Exception(f"Timeout after {timeout} minutes! Job may still be running. Visit URL: {visit_url}")


if __name__ == "__main__":
    run()
