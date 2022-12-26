terraform {
  required_providers {
    # We recommend pinning to the specific version of the Docker Provider you're using
    # since new versions are released frequently
    docker = {
      source  = "kreuzwerker/docker"
      version = "2.23.1"
    }
  }
}

# Configure the docker provider
provider "docker" {
}

# Create a docker image resource

resource "docker_image" "my_fastapi_res" {
  name = "my_fastapi"
  build {
    path = "."
    tag  = ["my_fastapi:develop"]
    build_arg = {
      name : "my_fastapi"
    }
    label = {
      author : "vbo"
    }
  }
}

# Create a docker container resource

resource "docker_container" "fastapi" {
  name    = "fastapi"
  image   = docker_image.my_fastapi_res.image_id

  ports {
    external = 8002
    internal = 8000
  }
}