import subprocess
from jinja2 import Template
import yaml
import os

BROMINMAX_FILE = "./libs/brominmax/getpb.py"

def get_config():
    try:
        with open("examples/operator-config.yaml", "r") as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        print("Error: YAML file not found.")
    except yaml.YAMLError as e:
        print("Error parsing YAML:", e)

config = get_config()
params = config['partitioning_params']
topics = config['topics']

script_args = ['python', BROMINMAX_FILE]
for param, value in params.items():
    script_args.append(f'--{param}')
    script_args.append(str(value))
result = subprocess.run(script_args, capture_output=True, text=True)
num_partitions,num_brokers = result.stdout.strip().split()

def render_cluster_yaml(num_brokers):

    template_str = open("resources/kubernetes-deployments/cluster.yaml").read()
    template = Template(template_str)

    variables = {
        "replicas": num_brokers,
    }

    return template.render(variables)

def render_topic_yaml(topics, num_partitions):
    template_str = open("resources/kubernetes-deployments/topic.yaml").read()
    template = Template(template_str)

    
    rendered_topics = []
    for topic in topics:
        variables = {
            "partitions": num_partitions,
            "replication_factor": params['r'],
            "topic_name": topic,
        }
        rendered_topics.append(template.render(variables))
    return "\n---\n".join(rendered_topics)



output_folder = "out"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "deployments.yaml")
with open(output_file, "w") as f:
    f.write(render_cluster_yaml(num_brokers))
    f.write("\n---\n")
    f.write(render_topic_yaml(topics, num_partitions))


print(f"Generated {output_file}")
