import requests

proms_range_url="http://172.31.255.83:9090/api/v1/query_range"
def query_prometheus_with_custom_range(
    start_day_str, 
    end_day_str, 
    job="k8s/exabits-h100/dcgm-exporter",
    pod_regex="eris-violet-12b-ex-.*",
    step_hours="1h",
):
    
    query = f'count(DCGM_FI_DEV_DEC_UTIL{{job="{job}",pod=~"{pod_regex}"}})'

    params = {
        "query": query,
        "start": f"{start_day_str}T00:00:00+08:00",
        "end": f"{end_day_str}T00:00:00+08:00",
        "step": step_hours,
    }
    
    data=query_prometheus(proms_range_url, params)
    if data is None or len(data)==0:
        return []
    return data[0]['values'][:-1]


def query_prometheus(prometheus_url, params, timeout=10):
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
        
        # Send GET request
        response = requests.get(prometheus_url, params=params, timeout=timeout)
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

if __name__ == "__main__":
    start_time = "2025-12-01"
    end_time = "2025-12-02"
    results = query_prometheus_with_custom_range(start_time, end_time)
    print(results)