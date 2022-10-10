from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.gcp.analytics import Dataproc
from diagrams.gcp.compute import ComputeEngine
from diagrams.gcp.iot import IotCore
from diagrams.gcp.storage import GCS
from diagrams.generic.blank import Blank
from diagrams.generic.device import Tablet
from diagrams.onprem.analytics import Spark
from diagrams.onprem.container import Docker
from diagrams.onprem.database import Postgresql
from diagrams.onprem.iac import Terraform
from diagrams.onprem.workflow import Airflow

common_cluster = {
    "bgcolor": "white",
    "fontsize": "17",
    "pencolor": "#2a9d8f",
    "penwidth": "1",
    "orientation": "portrait",
    "fontname": "Inter,BlinkMacSystemFont,Segoe UI",
    "margin": "20",
}

main_cluster = {
    "labelloc": "t",
    "fontsize": "28",
    "splines": "spline",
    "concentrate": "False",
}

with Diagram(
    "Architecture", show=False, graph_attr=main_cluster, direction="TB"
) as diag:
    with Cluster("IaC", graph_attr=common_cluster):
        tf = Terraform("", color="lightgray")
    with Cluster("Data warehouse", graph_attr=common_cluster):
        postgres = Postgresql("Postgres")

    with Cluster("Machine Learning", graph_attr=common_cluster):
        jupyter = Custom("TF-IDF/KMeans", "./images/jupyter.png")
    jobs_db = Tablet("JobsDB")

    with Cluster("Data Transformation", graph_attr=common_cluster):
        spark = Spark("")

    with Cluster("Data Lake", graph_attr=common_cluster):
        gcs = GCS("Cloud Storage")

    with Cluster("Workflow Orchestration", graph_attr=common_cluster):
        gce = ComputeEngine("Google Compute Engine")
        docker = Docker("Docker")
        airflow = Airflow("Airflow")

    tf >> Edge(style="dashed", color="lightgray") >> gce
    tf >> Edge(style="dashed", color="lightgray") >> postgres

    airflow >> jobs_db >> gcs
    airflow >> spark
    airflow >> postgres
    spark << gcs
    spark >> postgres
    postgres >> jupyter
    tf - Edge(penwidth="0") - airflow - Edge(penwidth="0") - spark
