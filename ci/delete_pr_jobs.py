import os
import argparse
import requests
import sys


def list_jobs(host: str, token: str):
    url = f"{host.rstrip('/')}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {token}"}

    jobs = []
    page_token = None

    while True:
        params = {}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        jobs.extend(data.get("jobs", []))

        page_token = data.get("next_page_token")
        if not page_token:
            break

    return jobs


def delete_job(host: str, token: str, job_id: int):
    url = f"{host.rstrip('/')}/api/2.1/jobs/delete"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, headers=headers, json={"job_id": job_id})
    resp.raise_for_status()


def main():
    parser = argparse.ArgumentParser(description="Delete Databricks jobs whose names start with a given prefix")
    parser.add_argument("--job_name_prefix", required=True, help="Prefix of job names to delete, e.g. PR-12_")

    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")

    prefix = args.job_name_prefix

    print(f"Listing jobs to delete with prefix: {prefix}")

    try:
        jobs = list_jobs(host, token)
    except Exception as e:
        print(f"Failed to list jobs: {e}", file=sys.stderr)
        sys.exit(1)

    to_delete = []
    for job in jobs:
        name = job.get("settings", {}).get("name")
        job_id = job.get("job_id")
        if name and name.startswith(prefix):
            to_delete.append((job_id, name))

    if not to_delete:
        print("No jobs found with the given prefix.")
        return

    print("Jobs to delete:")
    for job_id, name in to_delete:
        print(f"- {job_id}: {name}")

    for job_id, name in to_delete:
        print(f"Deleting job {job_id}: {name}")
        try:
            delete_job(host, token, job_id)
        except Exception as e:
            print(f"Failed to delete job {job_id} ({name}): {e}", file=sys.stderr)

    print("Job deletion completed ✅")


if __name__ == "__main__":
    main()
