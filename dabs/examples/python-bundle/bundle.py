"""
Python-defined bundle example
"""

def get_bundle():
    """Generate bundle configuration dynamically"""
    
    # Generate tasks dynamically
    environments = ["dev", "staging", "prod"]
    tasks = []
    
    for env in environments:
        tasks.append({
            "task_key": f"process_{env}",
            "notebook_task": {
                "notebook_path": "./src/process.py",
                "base_parameters": {
                    "environment": env
                }
            },
            "new_cluster": {
                "num_workers": 1 if env == "dev" else 2,
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge"
            }
        })
    
    return {
        "bundle": {
            "name": "python-defined-bundle"
        },
        "resources": {
            "jobs": {
                "dynamic_job": {
                    "name": "Dynamically Generated Job",
                    "tasks": tasks
                }
            }
        }
    }
