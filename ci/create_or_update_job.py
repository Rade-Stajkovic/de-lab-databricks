import os
import json
import argparse
import requests


def load_job_template(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def substitute_placeholders(job_conf: dict, pr_id: str, job_name_prefix: str | None = None) -> dict:
    # 1) Prefix job name (e.g. PR-1234_init_test_job)
    if job_name_prefix:
        job_conf["name"] = f"{job_name_prefix}{job_conf['name']}"

    # 2) Replace {{PR_ID}} in notebook parameters
    for task in job_conf.get("tasks", []):
        notebook_task = task.get("notebook_task")
        if notebook_task:
            base_params = notebook_task.get("base_parameters", {})
            for k, v in base_params.items():
                if isinstance(v, str) and "{{PR_ID}}" in v:
                    base_params[k] = v.replace("{{PR_ID}}", pr_id)
            notebook_task["base_parameters"] = base_params

    return job_conf


def create_job(job_conf: dict, host: str, token: str) -> dict:
    url = f"{host.rstrip('/')}/api/2.1/jobs/create"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, headers=headers, json=job_conf)

    if not resp.ok:
        print("Databricks API error response:")
        print("Status code:", resp.status_code)
        try:
            print("Body:", resp.json())
        except Exception:
            print("Body (raw):", resp.text)
        resp.raise_for_status()

    return resp.json()



def main():
    parser = argparse.ArgumentParser(description="Create Databricks job from JSON template")
    parser.add_argument("--job_template", required=True, help="Path to job JSON template (e.g. jobs/init_test_job.json)")
    parser.add_argument("--pr_id", required=True, help="PR environment id (e.g. pr_1234)")
    parser.add_argument("--job_name_prefix", required=False, help="Optional prefix for job name (e.g. PR-1234_)")

    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")

    job_conf = load_job_template(args.job_template)
    job_conf = substitute_placeholders(job_conf, args.pr_id, args.job_name_prefix)

    print("Final job config to create:")
    print(json.dumps(job_conf, indent=2))

    result = create_job(job_conf, host, token)
    print("Job created successfully:")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
