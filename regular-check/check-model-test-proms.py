import requests
from collections import defaultdict
import subprocess
import json
from datetime import datetime

cluster_metas=[
    {"context": "flow-do-nyc2", "vendor": "digitalocean"},
    {"context": "do-tor1", "vendor": "digitalocean-tor1"},
    ]

exclude_lists=["model-test-qwen3-embedding"]

def get_deployments_starting_with(prefix, context):
    try:
        # Command: Get all Deployments in the default namespace, output as JSON
        cmd = ['kubectl', 'get', 'deployments', '-o', 'json', '--context', context]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        deployment_names = []
        for item in data.get('items', []):
            name = item.get('metadata', {}).get('name', '')
            if name.startswith(prefix):
                deployment_names.append(name)
                
        return deployment_names
    except subprocess.CalledProcessError as e:
        print(f"Error executing kubectl command: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON output from kubectl: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []


def filter_deployments_by_age_and_replicas(deployment_names, context, days_threshold=60):
    """
    Filter deployments by creation time and replica count
    
    Parameters:
        deployment_names: List of deployment names
        context: kubectl context
        days_threshold: Days threshold, default is 60 days
        
    Returns:
        List of deployment names that meet the criteria (older than threshold and no replicas)
    """
    from datetime import datetime, timezone
    import subprocess
    import json
    
    filtered_deployments = []
    
    for dep_name in deployment_names:
        try:
            # Get detailed information for specific deployment
            cmd = ['kubectl', 'get', 'deployment', dep_name, '-o', 'json', '--context', context]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Error getting deployment {dep_name}: {result.stderr}")
                continue
                
            deployment_data = json.loads(result.stdout)
            
            # Get creation timestamp
            creation_str = deployment_data.get('metadata', {}).get('creationTimestamp', '')
            if not creation_str:
                raise ValueError(f"Could not get creation timestamp for deployment {dep_name}")
                
            # Get ready replicas count
            ready_replicas = deployment_data.get('status', {}).get('readyReplicas', 0)
            if ready_replicas is None:
                raise ValueError(f"Could not get ready replicas count for deployment {dep_name}")
            
            # Convert timestamp to datetime object
            try:
                # Handle the timezone-aware datetime from Kubernetes
                creation_time = datetime.fromisoformat(creation_str.replace("Z", "+00:00"))
            except ValueError:
                print(f"Invalid timestamp format for deployment {dep_name}: {creation_str}")
                continue
                
            # Use timezone-aware current time for comparison
            now_time = datetime.now(timezone.utc)
            if creation_time.tzinfo is None:
                # If creation_time is naive, make now_time naive too
                now_time = datetime.now()
            
            # Calculate time difference
            time_diff = now_time - creation_time
            days_old = time_diff.days
            
            # Check if older than threshold days and has no replicas
            if days_old > days_threshold and ready_replicas == 0:
                filtered_deployments.append(dep_name)
                
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON output for deployment {dep_name}: {e}")
        except ValueError as e:
            print(f"Value error for deployment {dep_name}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while processing deployment {dep_name}: {e}")
    
    return filtered_deployments


requests_query_tmpl='increase(vllm:e2e_request_latency_seconds_count{pod=~"model-test.*",vendor="$vendor"}[1h])'

def query_prometheus(prometheus_url, query, timeout=10):
    """
    Query Prometheus and return the results
    
    Parameters:
        prometheus_url: Base URL of the Prometheus server (e.g.: http://localhost:9090)
        query: PromQL query statement to execute
        timeout: Timeout in seconds
        
    Returns:
        Parsed query results, or None if an error occurs
    """
    try:
        # Construct the query URL
        url = f"{prometheus_url}/api/v1/query"
        
        # Set query parameters
        params = {
            "query": query
        }
        
        # Send GET request
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()  # Will raise HTTPError for bad status codes
        
        # Parse JSON response
        result = response.json()
        
        # Check response status
        if result.get("status") == "success":
            return result.get("data", {}).get("result", [])
        else:
            print(f"Query failed: {result.get('error', 'Unknown error')}")
            return None
            
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
    except requests.exceptions.ConnectionError:
        print("Connection error, please check if Prometheus address is correct")
    except requests.exceptions.Timeout:
        print(f"Query timed out after {timeout} seconds")
    except Exception as e:
        print(f"An error occurred: {e}")
    return None

def before_second_last_hyphen(pod_name: str) -> str:
    # Find all '-' positions
    hyphen_indices = [i for i, ch in enumerate(pod_name) if ch == '-']

    if len(hyphen_indices) < 2:
        raise ValueError("Input string must contain at least two hyphens ('-')")

    second_last_idx = hyphen_indices[-1]

    # Return the substring before that position (excluding '-')
    return pod_name[:second_last_idx]

def scale_deployment(deployment_name: str, replicas: int, context: str) -> bool:
    cmd = ["kubectl", "scale", "deployment", deployment_name, f"--replicas={replicas}", "--context", context]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        print("kubectl not found in PATH")
        return False
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        print(f"kubectl scale error (rc={proc.returncode}): {stderr}")
        return False
    print(f"Scaled deployment '{deployment_name}' to {replicas} replicas.")
    return True

def filter_old_deployments_without_replicas(deployments):
    """
    Filter out deployments that are older than 60 days and have no replicas
    
    Parameters:
        deployments: List of dictionaries containing deployment details
        
    Returns:
        List of deployment names that meet the criteria
    """
    from datetime import datetime, timedelta
    
    filtered_deployments = []
    
    for deployment in deployments:
        creation_str = deployment['creation_timestamp']
        
        # Convert string to datetime object
        try:
            creation_time = datetime.fromisoformat(creation_str.replace("Z", "+00:00"))
        except ValueError:
            print(f"Invalid timestamp format for deployment {deployment['name']}: {creation_str}")
            continue
            
        # Calculate time difference
        time_diff = datetime.now() - creation_time
        days_old = time_diff.days
        
        # Check if older than 60 days and has no replicas
        if days_old > 60 and deployment['ready_replicas'] == 0:
            filtered_deployments.append(deployment['name'])
            
    return filtered_deployments

def delete_resources_by_name(resource_name, context):
    """
    Delete Deployment, Service, and Ingress resources with the given name in the specified context.
    
    Args:
        resource_name (str): Name of the resources to delete (all three resource types use the same name)
        context (str): Kubernetes context to operate in
        
    Returns:
        dict: Status of deletion for each resource type
    """
    import subprocess
    
    deletion_status = {
        'deployment': False,
        'service': False,
        'ingress': False
    }
    
    # Define resource types to delete
    resource_types = ['deployment', 'service', 'ingress']
    
    for resource_type in resource_types:
        try:
            # Execute kubectl delete command for each resource type
            cmd = ['kubectl', 'delete', resource_type, resource_name, '--context', context, '--ignore-not-found=true']
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"Successfully deleted {resource_type} '{resource_name}' in context '{context}'")
                deletion_status[resource_type] = True
            else:
                print(f"Error deleting {resource_type} '{resource_name}': {result.stderr}")
        except Exception as e:
            print(f"Exception occurred while deleting {resource_type} '{resource_name}': {str(e)}")
    
    return deletion_status

if __name__ == "__main__":
    
    print(f"Starting script at: {datetime.now()}")
    # Prometheus server address, modify according to your actual environment
    PROMETHEUS_URL = "http://172.31.255.83:9090/"
    # PromQL query to execute
    #QUERY = "sum by(job)(increase(vllm:request_generation_tokens_count[24h]))"
    
    for meta in cluster_metas:
        context=meta["context"]
        vendor=meta["vendor"]
        print(f"Switching to context: {context}")

        query= requests_query_tmpl.replace("$vendor", vendor)
        # Execute the query
        print(f"Querying Prometheus: {query}")
        results = query_prometheus(PROMETHEUS_URL, query)
        print()
        print(results)
        print()
        stats=defaultdict(float)
        for item in results:
            pod_name = item['metric']['pod']
            #print(f"Original pod name: {pod_name}")
            modified_name = before_second_last_hyphen(pod_name)
            #print(f"Modified pod name: {modified_name}")
            item_value = float(item['value'][1])
            stats[modified_name] += item_value
        print(stats)
        print()

        deployments = get_deployments_starting_with("model-test", context)
        print(f"Deployments in context {context} starting with 'model-test':\n {deployments}")
        
        # Filter deployments using the new function
        old_deployments_without_replicas = filter_deployments_by_age_and_replicas(deployments, context)
        print(f"Deployments older than 60 days with no replicas in {context}: {old_deployments_without_replicas}")
        
        for one in exclude_lists:
            if one in deployments:
                print(f"Excluding deployment {one}")
                deployments.remove(one)

        for d in deployments:
            reclaim=True
            match_pod=False
            for p in stats:
                if d in p:
                    match_pod=True
                    if stats[p]!=0:
                        reclaim=False
            if match_pod and reclaim:
                print(f"Warning: Deployment {d} don't have any requests in the last hour")
                succeed=scale_deployment(d, 0, context)
                if not succeed:
                    raise ValueError(f"Failed to scale down deployment {d} in the cluster {context}")

        print("--------------------------------------------------")

        for d in old_deployments_without_replicas:
            print(f"Deleting old resources for deployment: {d}")
            delete_resources_by_name(d, context)
