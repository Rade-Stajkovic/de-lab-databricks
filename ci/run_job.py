import os
import time
import argparse
import requests
import sys
import json


def get_job_id_by_name(host: str, token: str, job_name: str) -> int:
    url = f"{host.rstrip('/')}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {token}"}

    job_id = None
    page_token = None

    while True:
        params = {}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        for job in data.get("jobs", []):
            name = job.get("settings", {}).get("name")
            if name == job_name:
                job_id = job.get("job_id")
                break

        if job_id is not None:
            break

        page_token = data.get("next_page_token")
        if not page_token:
            break

    if job_id is None:
        raise RuntimeError(f"Job with name '{job_name}' not found")

    return job_id


def run_job_and_wait(host: str, token: str, job_id: int, poll_interval: int = 15) -> None:
    run_now_url = f"{host.rstrip('/')}/api/2.1/jobs/run-now"
    runs_get_url = f"{host.rstrip('/')}/api/2.1/jobs/runs/get"

    headers = {"Authorization": f"Bearer {token}"}

    # 1) Trigger run
    resp = requests.post(run_now_url, headers=headers, json={"job_id": job_id})
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    print(f"Triggered run_id = {run_id} for job_id = {job_id}")

    # 2) Poll until completion
    while True:
        time.sleep(poll_interval)

        resp = requests.get(runs_get_url, headers=headers, params={"run_id": run_id})
        resp.raise_for_status()
        data = resp.json()
        state = data.get("state", {})
        life_cycle_state = state.get("life_cycle_state")
        result_state = state.get("result_state")
        state_message = state.get("state_message")

        print(f"Run {run_id} - life_cycle_state={life_cycle_state}, result_state={result_state}")

        if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result_state == "SUCCESS":
                print("Job run succeeded ✅")
                return
            else:
                print("Job run failed ❌")
                print("Full state:", json.dumps(state, indent=2))
                raise RuntimeError(f"Job run failed with state: {state_message or state}")


def main():
    parser = argparse.ArgumentParser(description="Run a Databricks job by name and wait for completion")
    parser.add_argument("--job_name", required=True, help="Databricks job name (e.g. PR-1234_init_test_job)")
    parser.add_argument("--poll_interval", type=int, default=15, help="Polling interval in seconds")

    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")

    try:
        job_id = get_job_id_by_name(host, token, args.job_name)
        print(f"Found job_id = {job_id} for job_name = {args.job_name}")
        run_job_and_wait(host, token, job_id, args.poll_interval)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

