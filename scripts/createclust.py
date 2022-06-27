import sys
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import dataproc_v1 as dataproc

def create_cluster(project_id, region, cluster_name):
    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options = {"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request = {"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    # Output a success message.
    print(f"Cluster created successfully: {result.cluster_name}")
    # [END dataproc_create_cluster]


if __name__ == "__main__":
    # if len(sys.argv) < 4:
    #     sys.exit("python create_cluster.py project_id region cluster_name")
    #
    # project_id = sys.argv[1]
    # region = sys.argv[2]
    # cluster_name = sys.argv[3]
    # create_cluster(project_id, region, cluster_name)
    project_id = "procrastinated-city-46778"
    region = "US"
    cluster_name = "Project_cluster"
    create_cluster(project_id,region,cluster_name)