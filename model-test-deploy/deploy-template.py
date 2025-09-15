from string import Template
import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--id")
parser.add_argument("--model")
parser.add_argument("--list", action="store_true",help="list all deployment which startswith 'model-test'")
# 新增用于调整副本的参数
parser.add_argument(
    "--scale-name",
    help="deployment name to scale (e.g. model-test-123)",
)
parser.add_argument(
    "--replicas",
    type=int,
    help="number of replicas to scale the specified deployment to (integer)",
)

def list_model_test_deployments() -> list[str]:
    """
    调用 kubectl 列出集群中所有 deployment 名称，返回以 'model-test' 开头的名称列表。
    """
    cmd = [
        "kubectl",
        "get",
        "deployments",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        print("kubectl not found in PATH")
        return []
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        print(f"kubectl error (rc={proc.returncode}): {stderr}")
        return []
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    print(lines[0])
    filtered = [name for name in lines if name.startswith("model-test")]
    return filtered

def scale_deployment(deployment_name: str, replicas: int) -> bool:
    """
    将指定名称的 deployment 的 replicas 设置为 replicas.
    返回 True 表示成功，False 表示失败。
    """
    if replicas < 0:
        print("replicas must be >= 0")
        return False
    cmd = ["kubectl", "scale", "deployment", deployment_name, f"--replicas={replicas}"]
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

def deploy_model(model_name: str, model_id: str) -> int:
    """
    使用给定的模型名称和 ID 部署模型。
    """
    template_path="./deployment-test-template.yaml"
    template = Template(open(template_path).read())

    temp_yaml=template.substitute(
        identifier=model_id,
        modelname=model_name
    )

    temp_path = "./temp-deployment.yaml"
    with open(temp_path, 'w') as temp:
        temp.write(temp_yaml)

    ret = subprocess.run(f"kubectl apply -f {temp_path}".split(" "))
    print(f"deployment return code {ret.returncode}")
    return ret.returncode

def main():
    args = parser.parse_args()
    if args.list:
        names = list_model_test_deployments()
        if not names:
            print("No deployments starting with 'model-test' found.")
        else:
            print("Deployments starting with 'model-test':")
            for n in names:
                print(n)
        raise SystemExit(0)

    # 处理 scale 操作
    if args.scale_name is not None or args.replicas is not None:
        if args.scale_name is None or args.replicas is None:
            raise ValueError("Both --scale-name and --replicas must be provided to scale a deployment")
        ok = scale_deployment(args.scale_name, args.replicas)
        raise SystemExit(0 if ok else 1)

    if not args.id or not args.model:
        raise ValueError('absent id or model')

    deploy_model(args.model, args.id)


if __name__ == "__main__":
    main()
