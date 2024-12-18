#!/usr/bin/env bash

current_dir=$(pwd)
script_dir=$(dirname "$0")
project_dir=$(dirname "$(dirname "$(realpath "$script_dir")")")
docker_file="Dockerfile"
container_name="unitycatalog"
container_version=$(cut -d '"' -f2 "$project_dir/docker/unity_catalog/version.sbt")
image_tag=$container_name:$container_version

echo "Changing directory to $project_dir/docker/unity_catalog"
cd "$project_dir/docker/unity_catalog" || exit

if [[ -r "$docker_file" ]]; then
    echo "Building $image_tag using the build definition from $docker_file"
    docker buildx build \
      --progress=plain \
      --no-cache \
      --build-arg unitycatalog_version="$container_version" \
      --tag "$image_tag" \
      -f "$docker_file" .
else
    echo "Dockerfile $docker_file not found in $project_dir. Exiting..."
    exit 1
fi

echo "Returning to base directory $current_dir"
cd "$current_dir" || exit