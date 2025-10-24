import requests
from collections import defaultdict
import subprocess
import json

cluster_metas=[
    {"context": "flow-do-nyc2", "vendor": "digitalocean"},
    {"context": "do-tor1", "vendor": "digitalocean-tor1"},
    ]

exclude_lists=["model-test-qwen3-embedding"]

def get_deployments_starting_with(prefix, context):
    try:
        # 命令：获取默认命名空间下的所有Deployments，输出为JSON
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
    # 找到所有 '-' 的位置
    hyphen_indices = [i for i, ch in enumerate(pod_name) if ch == '-']

    if len(hyphen_indices) < 2:
        raise ValueError("Input string must contain at least two hyphens ('-')")

    second_last_idx = hyphen_indices[-1]

    # 返回该位置之前的子字符串（不包含 '-')
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

if __name__ == "__main__":
    
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
        print(f"Deployments in context {context} starting with 'model-test\n': {deployments}")
        
        for one in exclude_lists:
            if one in deployments:
                print(f"Excluding deployment {d}")
                deployments.remove(d)

        for d in deployments:
            for p in stats:
                if stats[p]==0 and d in p:
                    print(f"Warning: Deployment {d} don't have any requests in the last hour")
                    succeed=scale_deployment(d, 0, context)
                    if not succeed:
                        raise ValueError(f"Failed to scale down deployment {d} in the cluster {context}")
                    break

        print("--------------------------------------------------")
