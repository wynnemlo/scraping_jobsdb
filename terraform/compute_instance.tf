resource "google_project_service" "enable-compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

# give service account compute admin permissions
resource "google_project_iam_member" "airflow-compute-iam" {
  project = var.project
  role = "roles/compute.admin"
  member = "serviceAccount:${var.airflow_service_account}"
}

resource "google_compute_instance" "airflow-machine" {
  name = "airflow-machine"
  machine_type = "e2-medium"
  zone = "us-west1-a"
  tags = ["allow-ssh", "allow-internal", "allow-http"]

  # startup script will install docker
  metadata = {
    ssh-keys = "${var.gce_ssh_user}:${file(var.gce_ssh_pub_key_file)}"
  }

  metadata_startup_script = "${file("startup.sh")}"

  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }
  
  # use the vpc_network we just created
  network_interface {
    subnetwork = google_compute_subnetwork.us-west1-subnet.id

    access_config {

    }
  }

  # attach service account above to compute instance
  service_account {
    email  = var.airflow_service_account
    scopes = ["cloud-platform"]
  }

  # export external ip to local machine for easy ssh
  provisioner "local-exec" {
    command = <<-EOT
      echo 'Host airflow-gce' > ${var.gce_ssh_config}
      echo '    HostName ${google_compute_instance.airflow-machine.network_interface.0.access_config.0.nat_ip}' >> ${var.gce_ssh_config}
      echo '    User wynnemlo' >> ${var.gce_ssh_config}
      echo '    IdentityFile ${var.gce_ssh_private_key_file}' >> ${var.gce_ssh_config}
      (Get-Content -path config) | Set-Content -Encoding UTF8 -Path config
    EOT
    interpreter = ["PowerShell", "-Command"]
  }
}

