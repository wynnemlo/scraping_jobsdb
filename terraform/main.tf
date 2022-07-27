terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

resource "google_compute_network" "vpc_network" {
  name = "my-network"
}

resource "google_compute_firewall" "allow_ssh" {
  name          = "allow-ssh"
  network       = google_compute_network.vpc_network.name
  target_tags   = ["allow-ssh"]
  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_iam_member" "airflow-compute-iam" {
  project = var.project
  role = "roles/compute.admin"
  member = "serviceAccount:${var.airflow_service_account}"
}

resource "google_compute_instance" "airflow" {
  name = "airflow-machine"
  machine_type = "e2-medium"
  zone = "us-west1-a"
  tags = ["allow-ssh"]

  metadata = {
    ssh-keys = "${var.gce_ssh_user}:${file(var.gce_ssh_pub_key_file)}"
  }

  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }
  
  network_interface {
    network = google_compute_network.vpc_network.name

    access_config {
    }
  }

  service_account {
    email  = var.airflow_service_account
    scopes = []
  }
}