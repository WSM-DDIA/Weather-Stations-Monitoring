#!/bin/bash

base_name="station"
extension=".yaml"
original_file="${base_name}1${extension}"

# Create copies with incremented numbers
for ((i=2; i<=10; i++)); do
    new_name="${base_name}${i}${extension}"
    cp "${base_name}$((i-1))${extension}" "$new_name"
    echo "Copied ${base_name}$((i-1))${extension} to $new_name"
done

