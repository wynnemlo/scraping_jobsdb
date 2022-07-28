resource "google_compute_network" "vpc-network" {
  name = "my-network"
  auto_create_subnetworks = false
  routing_mode = "REGIONAL"
}

resource "google_compute_subnetwork" "us-west1-subnet" {
  name = "us-west1-subnet"
  ip_cidr_range = var.us-west1-cidr
  region = "us-west1"
  network = google_compute_network.vpc-network.id
}

resource "google_compute_firewall" "allow-internal" {
  name = "allow-internal"
  network = google_compute_network.vpc-network.name
  allow {
    protocol = "icmp"
  }
  allow {
    protocol = "tcp"
    ports = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports = ["0-65535"]
  }
  source_ranges = ["${var.us-west1-cidr}"]
}

resource "google_compute_firewall" "allow-ssh" {
  name          = "allow-ssh"
  network       = google_compute_network.vpc-network.name
  target_tags   = ["allow-ssh"]
  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}